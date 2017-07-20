#!/usr/bin/env ruby

$: << File.expand_path('../lib', __FILE__)
require 'active_support/core_ext/string/inflections'
require 'json'
require 'stack_scaler'
require 'yaml'

def scaler
  @scaler ||= begin
    config_file = File.expand_path('../config/scaling.yml', __FILE__)
    config = YAML.load(File.read(config_file))
    StackScaler.new(config)
  end
end
