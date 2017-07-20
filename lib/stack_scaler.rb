require 'timeout'
require 'aws-sdk'
require 'faraday'
require 'zookeeper'

class StackScaler
  attr_accessor :logger, :config

  class Error < StandardError; end

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
    Aws::AutoScaling::Client.new.describe_auto_scaling_groups.auto_scaling_groups.each.with_object({}) do |g,h|
      h[g.tags.find { |t| t.key = 'name' }.value] = g.instances.length
    end
  end

  def collections
    if @config[:collections].nil? || @config[:collections].empty?
      @config[:collections] = JSON.parse(solr_client.get('admin/collections', action: 'LIST', wt: 'json').body)['collections']
    end
    @config[:collections]
  end

  def solr_backup
    location = '/data/backup'
    collections.each.with_object({}) do |collection, result|
      logger.info("Backing up collection: #{collection}")
      backup_name = "scaling_#{collection}_backup_#{timestamp}"
      response = solr_client.get('admin/collections', action: 'BACKUP', name: backup_name, collection: collection, location: location, wt: 'json')
      raise StackScaler::Error, "Backup of `#{collection}` failed:\n#{response.body}" unless JSON.parse(response.body)['success']
      result[collection] = backup_name
    end
  end

  def solr_restore
    location = '/data/backup'
    @config[:backups].each_pair do |collection, backup_name|
      logger.info("Restoring collection: #{collection}")
      solr_client.get('admin/collections', action: 'DELETE', name: collection, wt: 'json')
      response = solr_client.get('admin/collections', action: 'RESTORE', name: backup_name, collection: collection, location: location, maxShardsPerNode: 1, wt: 'json')
      raise StackScaler::Error, "Restore of `#{collection}` failed:\n#{response.body}" unless JSON.parse(response.body)['success']
    end
  end

  def scale_down
    auto_scaling_groups.each_pair do |environment, name|
      logger.info("Scaling #{environment} down to zero")
      asg = Aws::AutoScaling::AutoScalingGroup.new(name: name)
      asg.suspend_processes(scaling_processes: %w(Launch HealthCheck ReplaceUnhealthy AZRebalance AlarmNotification ScheduledActions AddToLoadBalancer))
      asg.disable_metrics_collection
      asg.update(min_size: 0, max_size: 0, desired_capacity: 0)
    end
  end

  def scale_up
    auto_scaling_groups.each_pair do |environment, name|
      capacity = capacity_for(environment)
      logger.info("Scaling #{environment} up to #{capacity.values_at(:min_size,:max_size,:desired_capacity).join('/')}")
      asg = Aws::AutoScaling::AutoScalingGroup.new(name: name)
      asg.resume_processes(scaling_processes: %w(Launch HealthCheck ReplaceUnhealthy AZRebalance AlarmNotification ScheduledActions AddToLoadBalancer))
      asg.enable_metrics_collection(granularity: '1Minute')
      asg.update(capacity)
    end
  end

  def suspend
    logger.info('Backing up solr/zookeeper collections')
    @config[:backups] = solr_backup
    logger.info('Suspending auto-scaling groups')
    scale_down
    logger.info('Suspend complete')
  end

  def resume
    solr_environment = auto_scaling_groups.keys.find { |k| k =~ /-solr/ }
    required_nodes = capacity_for(solr_environment)[:desired_capacity]

    logger.info('Resuming auto-scaling groups')
    scale_up
    wait_for_solr(required_nodes)
    solr_restore
    logger.info('Restore complete')
  end

  def wait_for_solr(count)
    logger.info("Waiting for zookeeper and #{count} live solr nodes")
    begin
      Timeout::timeout(600) do
        begin
          loop do
            response = solr_client.get('admin/collections', action: 'CLUSTERSTATUS', wt: 'json')
            break if response.status == 200 && JSON.parse(response.body)['cluster']['live_nodes'].length >= count
            sleep(30)
          end
        rescue
          sleep(30)
          retry
        end
      end
    rescue Timeout::Error
      raise StackScaler::Error, 'Zookeeper and solr failed to stabilize within 10 minutes'
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

    def fcrepo_client
      @fcrepo_client ||= Faraday.new("http://#{host_for('fcrepo')}/rest")
    end

    def solr_client
      @solr_client ||= Faraday.new("http://#{host_for('solr')}/solr")
    end

end
