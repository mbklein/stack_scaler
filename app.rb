#!/usr/bin/env ruby

$: << File.expand_path('../lib', __FILE__)
require 'json'
require 'stack_scaler'
require 'slack_io'
require 'yaml'
require 'sinatra'
require 'sinatra/config_file'

class ScalingApp < Sinatra::Base
  register Sinatra::ConfigFile

  config_file File.expand_path('../config/slack.yml', __FILE__)
  configure do
    set :server, :puma
  end

  helpers do
    def notifier
      if @notifier.nil?
        @notifier = Logger.new(SlackIO.new(token: settings.bot_token, channel: params[:channel_id]))
        @notifier.formatter = SlackIO::FORMATTER
      end
      @notifier
    end

    def config_file
      File.expand_path('../config/scaling.yml', __FILE__)
    end

    def scaler
      if @scaler.nil?
        config = YAML.load(File.read(config_file))
        @scaler = StackScaler.new(config, logger: notifier)
      end
      @scaler
    end

    def ephemeral(message)
      { response_type: 'ephemeral', text: message }.to_json
    end

    def control_command
      if params[:channel_id].to_s.sub(/^#/,'') == settings.control_channel_id
        yield
      else
        ephemeral("Cannot execute this command from `#{params[:channel_id]}`. Control commands must be sent from `##{settings.control_channel_name}`")
      end
    end

    def background
      child_pid = Process.fork do
        begin
          yield
        rescue StandardError => err
          notifier.fatal([err.class.name, err.message].join(': '))
        end
        Process.exit
      end
      Process.detach(child_pid)
    end
  end

  post '/webhook' do
    content_type 'application/json'

    return ephemeral('Bad Slack token') unless params[:token] == settings.slack_token

    begin
      case params[:text]
      when 'suspend'
        control_command do
          background do
            scaler.suspend
            File.open(config_file,'w') { |f| f.write(YAML.dump(scaler.config)) }
          end
          ephemeral('OK')
        end
      when 'resume'
        control_command do
          background { scaler.resume }
        end
        ephemeral('OK')
      when 'status'
        ephemeral("`#{params[:text]}` is not yet implemented")
      else
        ephemeral("`#{params[:text]}` is an unknown command")
      end
    rescue StandardError => err
      notifier.fatal([err.class.name, err.message].join(': '))
      raise
    end
  end

end
