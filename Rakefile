require File.expand_path('../console', __FILE__)
require 'term/ansicolor'

class Colorizer
  class << self
    include Term::ANSIColor
  end
end

desc 'Suspend stack'
task(:suspend) { scaler.suspend }

desc 'Resume stack'
task(:resume) { scaler.resume }

desc 'Get stack status'
task(:status) do
  Hash[scaler.status.sort].each_pair do |tag, data|
    color = data[:health].downcase
    color = 'bright_black' if color == 'gray'
    $stderr.puts(Colorizer.send(color.to_sym, "#{tag}: #{data[:count]} #{'instance'.pluralize(data[:count])} running"))
  end
end
