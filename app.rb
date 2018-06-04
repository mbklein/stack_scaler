#!/usr/bin/env ruby

$: << File.expand_path('../lib', __FILE__)
require 'active_support/core_ext/string/inflections'
require 'json'
require 'stack_scaler'
require 'slack_io'
require 'yaml'
require 'sinatra'
require 'sinatra/config_file'
require 'faraday'

class ScalingApp < Sinatra::Base
  register Sinatra::ConfigFile

  config_file File.expand_path('../config/slack.yml', __FILE__)
  configure do
    set :server, :puma
  end

  helpers do
    def slack_bot
      @slack_bot ||= ::Slack::Web::Client.new(token: settings.bot_token)
    end

    def user_to_notify
      if params[:user_id] && params[:user_name]
        "<@#{params[:user_id]}|#{params[:user_name]}>"
      elsif params[:user_id]
        "@#{params[:user_id]}"
      elsif params[:user_name]
        "#{params[:user_name]}"
      else
        nil
      end
    end

    def notifier
      if @notifier.nil?
        @notifier = Logger.new(SlackIO.new(token: settings.bot_token, channel: params[:channel_id], title: params[:text], user: user_to_notify))
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
        ephemeral("Cannot execute this command from `#{params[:channel_name]}`. Control commands must be sent from `##{settings.control_channel_name}`")
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

    def unescape_html(str)
      str.gsub(Regexp.union(*Rack::Utils::ESCAPE_HTML.values)) { |c| Rack::Utils::ESCAPE_HTML.find { |k,v| v == c }.first }
    end

    def subscription_response(payload)
      #original_message = payload['original_message']
      action = payload['actions'].first
      response_text = case action['name']
      when 'deny'
        ':negative_squared_cross_mark: _Subscription canceled_'
      when 'confirm'
        Faraday.get(unescape_html(action['value']))
        ':white_check_mark: _Subscription confirmed_'
      end

      { text: response_text }.to_json
      #{
      #  text: original_message['text'],
      #  attachments: [
      #    { text: response_text, attachment_type: 'default' }
      #  ]
      #}.to_json
    end
  end

  post '/action' do
    content_type 'application/json'
    payload = JSON.parse(params[:payload])
    case payload['callback_id']
    when 'subscription_confirm' then subscription_response(payload)
    end
  end

  post '/notify' do
    message = JSON.parse(request.body.read)
    if request.env['HTTP_X_AMZ_SNS_MESSAGE_TYPE'] == 'SubscriptionConfirmation'
      content = {
        text: "*Subscription Confirmation*\nSomeone subscribed this channel to `#{message['TopicArn']}`.",
        attachments: [
          {
            text: 'Please confirm or deny this subscription.',
            callback_id: 'subscription_confirm',
            attachment_type: 'default',
            fallback: "To confirm, please visit #{message['SubscribeURL']}",
            actions: [
              { name: 'confirm', text: 'Confirm', type: 'button', value: message['SubscribeURL'] },
              { name: 'deny', text: "Deny", type: 'button', value: 'DENY' }
            ]
          }
        ]
      }
      slack_bot.chat_postMessage(content.merge(channel: settings.control_channel_id, as_user: true))
    else
      text = "*#{message['Subject']}*\n#{message['Message']}"
      slack_bot.chat_postMessage(channel: settings.control_channel_id, text: text, as_user: true)
    end
    'OK'
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
          ephemeral('OK')
        end
      when 'force-down'
        control_command do
          background { scaler.scale_down }
          ephemeral('OK')
        end
      when 'force-up'
        control_command do
          background { scaler.scale_up }
          ephemeral('OK')
        end
        nil
      when 'replace-solr-leaders'
        control_command do
          background do
            scaler.replace_solr_leaders do |collection, shard, replica|
              notifier.info("Replacing #{collection}/#{shard}/#{replica}")
            end
          end
          ephemeral('OK')
        end
        nil
      when 'solr-status'
        "#{user_to_notify}: solr-status\n```\n#{scaler.solr_status}\n```"
      when 'status'
        Hash[scaler.status.sort].each_pair do |tag, data|
          notifier.info("#{tag}: #{data[:count]} #{'instance'.pluralize(data[:count])} running (#{data[:health].upcase})")
        end
        nil
      when /resolr (.+)/
        control_command do
          background { scaler.resolr($1) }
          ephemeral('OK')
        end
        nil
      else
        ephemeral("`#{params[:text]}` is an unknown command")
      end
    rescue StandardError => err
      notifier.fatal([err.class.name, err.message].join(': '))
      raise
    end
  end

end
