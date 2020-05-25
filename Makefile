PLUGIN_VERSION=1.0.1
PLUGIN_ID=warp10

plugin:
	cat plugin.json | json_pp > /dev/null
	cat custom-recipes/warp10update/recipe.json | json_pp > /dev/null
	cat custom-recipes/warpscript/recipe.json | json_pp > /dev/null
	cat python-connectors/warp10dataset/connector.json | json_pp > /dev/null
	cat parameter-sets/warp10connection/parameter-set.json | json_pp > /dev/null
	cat python-runnables/warp10exec/runnable.json | json_pp > /dev/null
	rm -rf dist
	./gradlew prepareForDistribution
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip custom-recipes java-lib js parameter-sets python-connectors python-lib python-runnables resource plugin.json
