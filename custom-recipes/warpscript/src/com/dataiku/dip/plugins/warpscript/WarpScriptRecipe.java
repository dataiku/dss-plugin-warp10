package com.dataiku.dip.plugins.warpscript;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.streamimpl.StreamColumnFactory;
import com.dataiku.dip.datalayer.streamimpl.StreamRow;
import com.dataiku.dip.datalayer.streamimpl.StreamRowFactory;
import com.dataiku.dip.datasets.DatasetCodes;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.exceptions.CodedException;
import com.dataiku.dip.output.Output;
import com.dataiku.dip.output.OutputWriter;
import com.dataiku.dip.plugin.BackendClient;
import com.dataiku.dip.plugin.CustomRecipe;
import com.dataiku.dip.plugin.ExecutionContext;
import com.dataiku.dip.recipes.consistency.RecipeCodes;
import com.dataiku.dip.utils.DKULogger;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.MemoryWarpScriptStack;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class WarpScriptRecipe implements CustomRecipe {

    private static final String WARP_TIMEUNITS_DEFAULT_VALUE = "=us";

    private JsonObject warpScriptProperties;
    private String warpScriptCode;
    private String timestampColumnName;
    private final List<String> valueColumnNames = new ArrayList<>();

    @Override
    public void init(String projectKey, JsonObject config, JsonObject pluginConfig, String resourceFolder) {
        warpScriptProperties = config.getAsJsonObject("properties");
        warpScriptCode = config.getAsJsonPrimitive("code").getAsString();
        timestampColumnName = config.getAsJsonPrimitive("timestampColumn").getAsString();
        for (JsonElement jsonElement : config.getAsJsonArray("valueColumns")) {
            valueColumnNames.add(jsonElement.getAsString());
        }
    }

    @Override
    public void run(ExecutionContext context, BackendClient client) throws Exception {
        // Set the initial time units config so this call succeeds
        WarpConfig.setProperties(new StringReader(Configuration.WARP_TIME_UNITS + WARP_TIMEUNITS_DEFAULT_VALUE));
        for (Map.Entry<String, JsonElement> property : warpScriptProperties.entrySet()) {
            WarpConfig.setProperty(property.getKey(), property.getValue().getAsString());
        }

        MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
        stack.maxLimits();

        List<GeoTimeSerie> gtsFromDataset = createGtsFromDataset(context);
        if (gtsFromDataset.size() == 1) {
            stack.push(gtsFromDataset.get(0));
        } else {
            stack.push(gtsFromDataset);
        }

        logger.info("Executing WarpScript");
        stack.execMulti(warpScriptCode);

        if (stack.depth() != 1) {
            throw new CodedException(RecipeCodes.ERR_RECIPE_GENERIC_ERROR, "WarpScript stack does not contain " +
                    "exactly 1 element after execution, but it contains " + stack.depth());
        }

        List<GeoTimeSerie> resultGts;
        Object poppedObject = stack.pop();
        if (poppedObject instanceof List) {
            resultGts = new ArrayList<>();
            List resultList = (List) poppedObject;
            for (Object item : resultList) {
                if (!(item instanceof GeoTimeSerie)) {
                    throw new CodedException(RecipeCodes.ERR_RECIPE_GENERIC_ERROR,
                            "Found a " + item.getClass().getName() + " in the resulting List, only GeoTimeSeries " +
                                    "objects are allowed");
                }

                resultGts.add((GeoTimeSerie) item);
            }
        } else if (poppedObject instanceof GeoTimeSerie) {
            resultGts = Collections.singletonList((GeoTimeSerie) poppedObject);
        } else {
            throw new CodedException(RecipeCodes.ERR_RECIPE_GENERIC_ERROR, "Element on the stack is a " +
                    poppedObject.getClass().getName() + ", it must be either a List or a GeoTimeSeries object");
        }

        writeGtsToDataset(resultGts, context);
    }

    private List<GeoTimeSerie> createGtsFromDataset(ExecutionContext context) throws Exception {
        ExecutionContext.InputInfo inputInfo = context.getInputs().get(0);

        Schema schema = inputInfo.getSchema();
        if (schema.getColumn(timestampColumnName) == null) {
            throw new CodedException(DatasetCodes.ERR_DATASET_SCHEMA_TO_DATA_MISMATCH,
                    "Input dataset schema must contain a timestamp column named " + timestampColumnName);
        }

        StreamColumnFactory columnFactory = new StreamColumnFactory();
        Column timestampColumn = columnFactory.column(timestampColumnName);

        Map<Column, Type> valuesColumnTypes = new HashMap<>();
        for (String valueColumnName : valueColumnNames) {
            if (schema.getColumn(valueColumnName) == null) {
                throw new CodedException(DatasetCodes.ERR_DATASET_SCHEMA_TO_DATA_MISMATCH,
                        "Input dataset schema must contain a column named " + valueColumnName);
            }

            Type type = schema.getColumn(valueColumnName).getType();
            Column valueColumn = columnFactory.column(valueColumnName);
            valuesColumnTypes.put(valueColumn, type);
        }

        List<Column> labels = new ArrayList<>();
        // The remaining columns are labels
        for (SchemaColumn column : schema.getColumns()) {
            String columnName = column.getName();
            if (!timestampColumnName.equals(columnName) && !valueColumnNames.contains(columnName)) {
                logger.infoV("Detected column %s as a label", columnName);
                labels.add(columnFactory.column(columnName));
            }
        }

        logger.infoV("Converting %s into a GeoTimeSeries and pushing on the stack", inputInfo.getId());

        Map<Metadata, GeoTimeSerie> geoTimeSeriesMap = new HashMap<>();
        inputInfo.pullFromDataset(new GtsProcessorOutput(geoTimeSeriesMap, timestampColumn, valuesColumnTypes, labels),
                columnFactory, new StreamRowFactory());
        return new ArrayList<>(geoTimeSeriesMap.values());
    }

    private static class GtsProcessorOutput implements ProcessorOutput {

        private final Map<Metadata, GeoTimeSerie> geoTimeSeriesMap;
        private final Map<Column, Type> valueColumnTypes;
        private final Column timestampColumn;
        private final List<Column> labelColumns;

        GtsProcessorOutput(Map<Metadata, GeoTimeSerie> geoTimeSeriesMap,
                           Column timestampColumn,
                           Map<Column, Type> valueColumnTypes,
                           List<Column> labelColumns) {
            this.geoTimeSeriesMap = geoTimeSeriesMap;
            this.valueColumnTypes = valueColumnTypes;
            this.timestampColumn = timestampColumn;
            this.labelColumns = labelColumns;
        }

        @Override
        public void emitRow(Row row) {
            long ts = Long.parseLong(row.get(timestampColumn));

            Map<String, String> labels = new HashMap<>();
            for (Column labelColumn : labelColumns) {
                String labelValue = row.get(labelColumn);
                if (StringUtils.isNotBlank(labelValue)) {
                    labels.put(labelColumn.getName(), labelValue);
                }
            }

            for (Map.Entry<Column, Type> columnEntry : valueColumnTypes.entrySet()) {
                Column column = columnEntry.getKey();
                Type type = columnEntry.getValue();

                String valueString = row.get(column);
                if (StringUtils.isBlank(valueString)) {
                    continue;
                }

                Object value = valueString;
                if (type.isInteger()) {
                    value = Long.valueOf(valueString);
                } else if (type.isFloatingPoint()) {
                    value = Double.valueOf(valueString);
                } else if (type == Type.BOOLEAN) {
                    value = Boolean.valueOf(valueString);
                }

                Metadata gtsMetadata = new Metadata().setName(column.getName()).setLabels(labels);
                GeoTimeSerie gts = geoTimeSeriesMap.get(gtsMetadata);
                if (gts == null) {
                    gts = new GeoTimeSerie();
                    gts.setMetadata(gtsMetadata);
                    geoTimeSeriesMap.put(gtsMetadata, gts);
                }
                GTSHelper.setValue(gts, ts, value);
            }
        }

        @Override
        public void lastRowEmitted() {
            // Nothing to do.
        }

        @Override
        public void cancel() {
            // Nothing to do.
        }

        @Override
        public void setMaxMemoryUsed(long l) {
            // Nothing to do.
        }
    }

    private void writeGtsToDataset(List<GeoTimeSerie> resultGts, ExecutionContext context) throws Exception {
        Schema schema = new Schema().withColumn(timestampColumnName, Type.BIGINT);

        StreamColumnFactory columnFactory = new StreamColumnFactory();
        Column timestampColumn = columnFactory.column(timestampColumnName);

        Set<String> labelColumns = new HashSet<>();

        for (GeoTimeSerie gts : resultGts) {
            GeoTimeSerie.TYPE type = gts.getType();
            Type dssType;
            switch (type) {
                case LONG:
                    dssType = Type.BIGINT;
                    break;
                case DOUBLE:
                    dssType = Type.DOUBLE;
                    break;
                case BOOLEAN:
                    dssType = Type.BOOLEAN;
                    break;
                case STRING:
                case UNDEFINED:
                default:
                    dssType = Type.STRING;
                    break;
            }
            schema.withColumn(gts.getName(), dssType);
            labelColumns.addAll(gts.getLabels().keySet());
        }

        for (String labelColumn : labelColumns) {
            schema.withColumn(labelColumn, Type.STRING);
        }

        ExecutionContext.OutputInfo outputInfo = context.getOutputs().get(0);
        if (outputInfo.getWriteMode() == Output.WriteMode.OVERWRITE) {
            outputInfo.clear();
        }

        outputInfo.setSchema(schema);

        logger.infoV("Writing resulting GeoTimeSeries to %s", outputInfo.getId());

        OutputWriter outputWriter = outputInfo.pushToDataset();
        outputWriter.init(columnFactory);

        // TODO: This method of merging and iterating through multiple GTS can probably be improved.
        //  Using GTSHelper.valueAtTick means that this is nlog(n). Faster ways are possible, for example if you assume
        //  that the values are stored in order in the GTS, then you can simultaneously iterate through each GTS,
        //  inserting the values for the current lowest timestamp found, keeping in mind that multiple GTS could have a
        //  value at that timestamp.
        TreeSet<Long> ticks = new TreeSet<>();
        for (GeoTimeSerie gts : resultGts) {
            if (gts.size() > 0) {
                Collections.addAll(ticks, ArrayUtils.toObject(GTSHelper.getTicks(gts)));
            }
        }

        for (long tick : ticks) {
            StreamRow row = new StreamRow().with(timestampColumn, tick);
            for (GeoTimeSerie gts : resultGts) {
                Object value = GTSHelper.valueAtTick(gts, tick);
                if (value == null) {
                    continue;
                }
                Column valueColumn = columnFactory.column(gts.getName());
                switch (gts.getType()) {
                    case LONG:
                        row.with(valueColumn, (long) value);
                        break;
                    case DOUBLE:
                        row.with(valueColumn, (double) value);
                        break;
                    case BOOLEAN:
                        row.with(valueColumn, (boolean) value);
                        break;
                    case STRING:
                    case UNDEFINED:
                    default:
                        row.with(valueColumn, (String) value);
                        break;
                }

                for (Map.Entry<String, String> label : gts.getLabels().entrySet()) {
                    Column labelColumn = columnFactory.column(label.getKey());
                    row.with(labelColumn, label.getValue());
                }
            }
            outputWriter.emitRow(row);
        }

        outputWriter.lastRowEmitted();
    }

    @Override
    public void abort() {
        // Not supported.
    }

    private static final DKULogger logger = DKULogger.getLogger("warpscript.recipe");
}
