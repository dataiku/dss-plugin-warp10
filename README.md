# Warp10 Plugin 

This plugin adds a Warp10 connector, two WarpScript recipes, and a WarpScript macro.

## Build

Make sure you have a `DKUINSTALLDIR` environment variable set.

```shell script
make all
```

This will build a zip file in the `dist` directory that can then be installed in DSS.

## Parameter Set

* Warp10 hostname
* Warp10 port
* Warp10 read token
* Warp10 write token

## The Warp10 connector

Explore Warp10 data as a DSS dataset

## The WarpScript recipe (on Warp10)

Execute WarpScript on Warp10 to create and write a new GTS on Warp10.

## The WarpScript recipe (on DSS)

Execute WarpScript directly on DSS on a DSS dataset. This has no dependency on Warp10.

### Notes

Additional runtime dependencies are needed for WarpScript to be executed in DSS. These are placed in `java-lib` by the `gradle` build file, and included in the zip distribution. DSS will then look inside this directory for JARs to add to the classpath when executing a WarpScript recipe.

## The WarpScript macro

Execute arbitrary WarpScript on Warp10.

## Unit tests

To run the unit tests in `python-test`, from the root directory of the plugin, run (requires `pytest` to be installed):
```shell script
pytest
```
