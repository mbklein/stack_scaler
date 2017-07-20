require 'slack-ruby-client'

class SlackIO < IO

  FORMATTER = proc { |severity, datetime, progname, msg|
    result = datetime.strftime("%Y-%m-%d %H:%M:%S.%3N ".freeze)
    result += "[#{progname}] " unless progname.nil?
    result += "#{severity}: " unless ['INFO','ANY'].include?(severity)
    result + msg
  }

  def initialize(token:, channel:, title: nil, user: nil)
    @title = title
    @user = user
    @slack = ::Slack::Web::Client.new(token: token)
    @channel = channel
    @io = StringIO.new('')
  end

  def write(str)
    @io << "\n" unless @message.nil?
    @io << "#{str}"
    if @message.nil?
      @message = @slack.chat_postMessage(channel: @channel, text: message, as_user: true, parse: 'full')
    else
      @slack.chat_update(ts: @message['ts'], channel: @message['channel'], text: message, as_user: true, parse: 'full')
    end
  end

  private

    def message
      msg = ''
      msg << "#{[@user, @title].compact.join(': ')}\n" if @user || @title
      msg << "```\n#{@io.string.strip}\n```"
      msg
    end

end
