all:
	@ redis-server test/redis.conf
	@ resty -I lib test/example.lua
	@ resty -I lib test/example2.lua
	@ resty -I lib test/fuzzy.lua
	@ redis-cli shutdown
