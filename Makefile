ifdef suites
	suites_opt = suites=$(suites)
endif

eunit_dir = .eunit

all: compile

compile:
	@./rebar compile

# "set -o pipefail" is here to ensure that the exit status of the rebar call gets returned past the pipe if it fails.
check: compile
	@mkdir -p $(eunit_dir)
	@rm -rf $(eunit_dir)/*.log
	@bash -c "set -o pipefail && \
	  ./rebar eunit skip_deps=true $(suites_opt) \
	  | tee $(eunit_dir)/test.log"

clean:
	@./rebar clean
	@rm -rf erl_crash.dump $(eunit_dir)
