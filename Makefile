
test:
	go test

integ:
	INTEG_TESTS=true AMQP_HOST=172.16.0.100 go test

cov:
	INTEG_TESTS=true AMQP_HOST=172.16.0.100 gocov test github.com/armon/relay | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

