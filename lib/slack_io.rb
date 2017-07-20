require 'slack-ruby-client'

class SlackIO < IO

  FORMATTER = proc { |severity, datetime, progname, msg|
    result = datetime.strftime("%Y-%m-%d %H:%M:%S.%3N ".freeze)
    result += "[#{progname}] " unless progname.nil?
    result += "#{severity}: " unless ['INFO','ANY'].include?(severity)
    result + msg
  }

  def initialize(token:, channel:, title: nil, user: nil)
    @index = 1
    @title = title
    @user = user
    @slack = ::Slack::Web::Client.new(token: token)
    @channel = channel
    @io = StringIO.new('')
  end

  def write(str)
    if @io.string.lines.length > 60
      @slack.chat_update(ts: @message['ts'], channel: @message['channel'], text: message(true), as_user: true)
      @message = nil
      @io = StringIO.new('')
      @index += 1
    end
    @io << "\n" unless @message.nil?
    @io << "#{str}"
    if @message.nil?
      @message = @slack.chat_postMessage(channel: @channel, text: message, as_user: true)
    else
      @slack.chat_update(ts: @message['ts'], channel: @message['channel'], text: message, as_user: true)
    end
  end

  private

    def message(force_part = false)
      msg = ''
      if @user || @title
        msg << "#{[@user, @title].compact.join(': ')}"
        msg << " [Part #{@index}]" if @index > 1 or force_part
        msg << "\n"
      else
        msg << "Part #{@index}" if @index > 1
      end
      msg << "```\n#{@io.string.strip}\n```"
      msg
    end

end
