require 'timeout'
require 'aws-sdk'
require 'faraday'
require 'ostruct'
require 'zookeeper'

class StackScaler
  attr_accessor :logger, :config

  class Error < StandardError; end
  class ConnectionError < Error; end

  def initialize(config, logger: nil)
    @config = config
    @logger = logger || Logger.new($stderr)
  end

  def auto_scaling_groups
    @auto_scaling_groups ||= Aws::AutoScaling::Client.new.describe_auto_scaling_groups.auto_scaling_groups.each.with_object({}) do |g,h|
      h[g.tags.find { |t| t.key = 'name' }.value] = g.auto_scaling_group_name
    end
  end

  def status
    environment_info = Aws::ElasticBeanstalk::Client.new.describe_environments
    environments = environment_info.environments
    Aws::AutoScaling::Client.new.describe_auto_scaling_groups.auto_scaling_groups.each.with_object({}) do |g,h|
      environment_name = g.tags.find { |t| t.key = 'name' }.value
      environment_info = environments.find { |e| e.environment_name == environment_name }
      health_color = environment_info&.health || 'Unknown'
      h[environment_name] = { count: g.instances.length, health: health_color }
    end
  end

  def collections
    if @config[:collections].nil? || @config[:collections].empty?
      @config[:collections] = solr_collections_api(:list).collections
    end
    @config[:collections]
  end

  def solr_backup
    location = '/data/backup'
    collections.each.with_object({}) do |collection, result|
      logger.info("Backing up collection: #{collection}")
      backup_name = "scaling_#{collection}_backup_#{timestamp}"
      solr_client.get("#{collection}/update", optimize: true)
      response = solr_collections_api(:backup, name: backup_name, collection: collection, location: location)
      raise StackScaler::Error, "Backup of `#{collection}` failed:\n#{response.to_h.to_json}" unless response.success || (response.responseHeader.status == 0)
      result[collection] = backup_name
    end
  end

  def solr_restore
    location = '/data/backup'
    active_nodes = solr_collections_api(:clusterstatus).cluster.live_nodes.length
    @config[:backups].each_pair do |collection, backup_name|
      logger.info("Restoring collection: #{collection}")
      solr_collections_api(:delete, name: collection)
      response = solr_collections_api(:restore, name: backup_name, collection: collection, location: location, maxShardsPerNode: 1, replicationFactor: active_nodes)
      raise StackScaler::Error, "Restore of `#{collection}` failed:\n#{response.to_h.to_json}" unless response.success || (response.responseHeader.status == 0)
      # while solr_collections_api(:clusterstatus, collection: collection).cluster.collections[collection].shards.shard1.replicas.to_h.length < active_nodes
      #   solr_collections_api(:addreplica, collection: collection, shard: 'shard1')
      # end
    end
  end

  def solr_replicate
    cluster = solr_collections_api(:clusterstatus).cluster
    active_nodes = cluster.live_nodes.length
    cluster.collections.each_pair do |name, details|
      replicas = replicas = details.shards.shard1.replicas.to_h.values.group_by(&:state)
      down_count = replicas['down'].length
      collection_nodes = replicas['active'].length
      nodes_needed = (active_nodes - collection_nodes)
      logger.info("#{name}: Removing #{down_count} dead replicas and adding #{nodes_needed} new replicas")
      solr_collections_api(:deletereplica, collection: name, count: down_count, shard: 'shard1')
      nodes_needed.times do
        solr_collections_api(:addreplica, collection: name, shard: 'shard1')
      end
      solr_collections_api(:reload, name: name)
    end
  end

  def scale_down
    auto_scaling_groups.each_pair do |environment, name|
      capacity = environment =~ /zookeeper|solr/ ? 1 : 0
      logger.info("Scaling #{environment} down to #{capacity}")
      asg = Aws::AutoScaling::AutoScalingGroup.new(name: name)
      asg.suspend_processes(scaling_processes: %w(Launch HealthCheck ReplaceUnhealthy AZRebalance AlarmNotification ScheduledActions AddToLoadBalancer))
      asg.disable_metrics_collection
      asg.update(min_size: capacity, max_size: capacity, desired_capacity: capacity)
    end
  end

  def scale_up_zookeeper
    scale_up match: /-zookeeper/
    wait_for(:zookeeper)
  end

  def scale_up_solr
    scale_up match: /-solr/
    wait_for(:solr)
  end

  def scale_up_fcrepo
    scale_up match: /-fcrepo/
  end

  def scale_up_webapps
    scale_up match: /-(webapp|workers)/
  end

  def scale_up(match: nil)
    auto_scaling_groups.each_pair do |environment, name|
      next unless match.nil? || (environment =~ match)
      capacity = capacity_for(environment)
      logger.info("Scaling #{environment} up to #{capacity.values_at(:min_size,:max_size,:desired_capacity).join('/')}")
      asg = Aws::AutoScaling::AutoScalingGroup.new(name: name)
      asg.resume_processes(scaling_processes: %w(Launch HealthCheck ReplaceUnhealthy AZRebalance AlarmNotification ScheduledActions AddToLoadBalancer))
      asg.enable_metrics_collection(granularity: '1Minute')
      asg.update(capacity)
    end
  end

  def suspend
#    logger.info('Backing up solr/zookeeper collections')
#    @config[:backups] = solr_backup
    logger.info('Suspending auto-scaling groups')
    scale_down
    logger.info('Suspend complete')
  end

  def resume
    logger.info('Resuming auto-scaling groups')
    scale_up_fcrepo
    scale_up_zookeeper
    scale_up_solr
    solr_replicate
#    solr_restore
    scale_up_webapps
    logger.info('Restore complete')
  end

  def wait_for(env)
    environment = auto_scaling_groups.keys.find { |k| k.ends_with?("-#{env.to_s}") }
    required_nodes = capacity_for(environment)[:desired_capacity]
    send("wait_for_#{env}".to_sym, required_nodes)
  end

  def wait_for_zookeeper(count)
    logger.info("Waiting for #{count} synced zookeeper ensemble nodes")
    begin
      Timeout::timeout(600) do
        loop do
          state = zookeeper_state
          break if state['zk_server_state'] == 'leader' && state['zk_synced_followers'].to_i >= (count-1)
          sleep(10)
        end
      end
    rescue Timeout::Error
      raise StackScaler::Error, 'Zookeeper failed to stabilize within 10 minutes'
    end
  end

  def wait_for_solr(count)
    logger.info("Waiting for #{count} live solr nodes")
    begin
      Timeout::timeout(600) do
        begin
          loop do
            response = zookeeper_client.get_children(path: '/live_nodes')
            break if response[:stat].numChildren >= count
            sleep(30)
          end
        rescue
          sleep(30)
          retry
        end
      end
    rescue Timeout::Error
      raise StackScaler::Error, 'Solr failed to stabilize within 10 minutes'
    end
  end

  private

    def timestamp
      Time.now.strftime('%Y%m%d%H%M%S')
    end

    def capacity_for(environment)
      { min_size: 1, max_size: 2, desired_capacity: 1 }.merge(@config[:scaling][environment] || {})
    end

    def host_for(name)
      "#{name}.repo.vpc.#{@config[:dns_zone]}"
    end

    def zookeeper_client
      @zookeeper_client ||= Zookeeper.new("#{host_for('zk')}:2181")
    end

    def zookeeper_state
      begin
        Timeout::timeout(5) do
          TCPSocket.open(host_for('zk'), 2181) do |sock|
            sock.puts 'mntr'
            Hash[sock.read.lines.collect { |l| l.chomp.split(/\t/) }]
          end
        end
      rescue
        { "zk_state" => "unavailable" }
      end
    end

    def fcrepo_client
      @fcrepo_client ||= Faraday.new("http://#{host_for('fcrepo')}/rest")
    end

    def solr_client
      @solr_client ||= Faraday.new("http://#{host_for('solr')}/solr")
    end

    def solr_collections_api(action, args = {})
      params = args.merge(action: action.to_s.upcase, wt: 'json')
      response = solr_client.get('admin/collections', params)
      to_ostruct(JSON.parse(response.body))
    end

    def to_ostruct(hash)
      o = OpenStruct.new(hash)
      hash.each.with_object(o) do |(k,v), o|
        o.send(:"#{k}=", to_ostruct(v)) if v.respond_to? :each_pair
      end
      o
    end
end
