class SlackIO < IO

  FORMATTER = proc { |severity, datetime, progname, msg|
    result = ''
    result += "[#{progname}] " unless progname.nil?
    result += "#{severity}: " unless ['INFO','ANY'].include?(severity)
    result + msg
  }

  def initialize(webhook_url)
    @slack = ::Slack::Notifier.new(webhook_url)
  end

  def write(str)
    @slack.ping(str)
  end

end
