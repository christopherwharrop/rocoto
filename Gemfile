# frozen_string_literal: true

source "https://rubygems.org"

ruby ">= 3.2.0"

gem "nokogiri", "~> 1.16" # Uses precompiled libxml2 on supported platforms
gem "open4", "~> 1.3"
gem "sqlite3", "~> 1.7"
gem "thread", "~> 0.2"

# Legacy Ruby shim libraries for compatibility
# TODO: These can likely be removed after refactoring parsedate usage to use Date.parse
gem "rubysl-date", "~> 1.0"
gem "rubysl-parsedate", "~> 1.0"

group :development, :test do
  gem "rake", "~> 13.0"
  gem "rspec", "~> 3.13"
end

group :development do
  gem "rubocop", "~> 1.66", require: false
  gem "rubocop-rake", "~> 0.6", require: false
  gem "rubocop-rspec", "~> 3.0", require: false
end

group :test do
  gem "simplecov", "~> 0.22", require: false
end
