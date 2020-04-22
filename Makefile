PLUGIN_VERSION=0.0.1
PLUGIN_ID=warp10

all:
	cat plugin.json | json_pp > /dev/null
	cat custom-recipes/warp10update/recipe.json | json_pp > /dev/null
	cat custom-recipes/warpscript/recipe.json | json_pp > /dev/null
	cat python-connectors/warp10dataset/connector.json | json_pp > /dev/null
	cat parameter-sets/warp10connection/parameter-set.json | json_pp > /dev/null
	cat python-runnables/warp10exec/runnable.json | json_pp > /dev/null
	rm -rf dist
	./gradlew prepareForDistribution
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip python-connectors parameter-sets python-lib java-lib custom-recipes python-runnables js resource plugin.json

reinstall-in-dss: all
	${DIP_HOME}/bin/dku install-plugin dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip -u
	${DIP_HOME}/bin/dss restart backend
