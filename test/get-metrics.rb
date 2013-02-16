require './helper'
require 'time'

class GetMetricsTest < L2metTest

  def test_l2met_groups_data_with_resolution
    num_vals = 1
    stop = Time.now
    curr = stop - (60*60) # start at 1 hour ago
    while curr < stop
      num_vals.times do
        d = 'measure="l2met-simple-test"'
        msg = "<0>1 #{Time.at(curr).iso8601} 1234 token shuttle - - #{d}"
        out = `echo '#{msg}' | log-shuttle -skip-headers`
      end
      curr += 60
    end
    # Wait for l2met to flush bucket.
    sleep(1)
    metrics = get('metrics/l2met-simple-test?limit=1&resolution=60')
    assert_equal(1, metrics.length)
    metric = metrics.first
    assert_equal('l2met-simple-test', metric["name"])
    assert_equal(1, metric["mean"])
  end

end
