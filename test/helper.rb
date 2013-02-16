$stdout.sync = $stderr.sync = true

if ENV['LOGPLEX_URL'].nil?
  puts('Must set LOGPLEX_URL.')
  exit(1)
end

require 'minitest/unit'
require 'json'
require 'net/http'
require 'uri'

MiniTest::Unit.autorun

class L2metTest < MiniTest::Unit::TestCase

  def setup
    puts(`psql metrics -c 'delete from metrics'`)
  end

  def get(path)
    uri = URI.parse([ENV['L2MET_URL'], path].join('/'))
    http = Net::HTTP.new(uri.host, uri.port)
    request = Net::HTTP::Get.new(uri.request_uri)
    request.basic_auth(uri.user, uri.password)
    response = http.request(request)
    buckets = JSON.parse(response.body)
  rescue JSON::ParserError => e
    puts uri.request_uri
    puts response.body
  end

end
