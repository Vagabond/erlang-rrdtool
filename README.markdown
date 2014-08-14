About
=====

Erlang-rrdtool is a simple module to allow rrdtool to be treated like an erlang
port via its 'remote control' mode (`rrdtool -`).

You should probably use https://github.com/archaelus/errd instead.

Usage
=====

Creating a rrd (example #1 from the rrdcreate manpage):

    1> {ok, Pid} = rrdtool:start().
    {ok,<0.221.0>}
    2> rrdtool:create(Pid, "temperature.rrd", [{"temp", 'GAUGE', [600, -273, 5000]}],
        [{'AVERAGE', 0.5, 1, 1200}, {'MIN', 0.5, 12, 2400}, {'MAX', 0.5, 12, 2400},
        {'AVERAGE', 0.5, 12, 2400}]).
    ok

Updating a RRD:

    3> rrdtool:update(Pid, "temperature.rrd", [{"temp", 50}]).
    ok
    4> rrdtool:update(Pid, "temperature.rrd", [{"temp", 75}], now()).
    ok

Rebar
=====

To use rrdtool in your projects, add a dependency to your rebar.config:

    {deps, [{rrdtool, ".*",
             {git, "git://github.com/Vagabond/erlang-rrdtool.git", "master"}}]}.
