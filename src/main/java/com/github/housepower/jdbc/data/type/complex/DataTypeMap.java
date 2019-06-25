package com.github.housepower.jdbc.data.type.complex;

import com.github.housepower.jdbc.connect.PhysicalInfo;
import com.github.housepower.jdbc.data.DataTypeFactory;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.misc.SQLLexer;
import com.github.housepower.jdbc.misc.Validate;
import com.github.housepower.jdbc.serializer.BinaryDeserializer;
import com.github.housepower.jdbc.serializer.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DataTypeMap implements IDataType {


    private final String name;
    private final Map defaultvalue;
    private final IDataType keyType;
    private final IDataType valueType;


    public DataTypeMap(String name, IDataType keyType, IDataType valueType) {

        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.defaultvalue = null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return 123;
    }

    @Override
    public Object defaultValue() {
        return defaultvalue;
    }

    @Override
    public Class javaTypeClass() {
        return Map.class;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Object deserializeTextQuoted(SQLLexer lexer) throws SQLException {
        return null;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {

    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return null;
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {

    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Map[] data = new Map[rows];

        if (rows == 0) {
            return data;
        }else {
            Long[] offsets = new Long[rows];

            for (int i = 0; i < rows; i++) {
                Long keySize = deserializer.readLong();
                offsets[i] = keySize;
            }

            Object[] keys = new Object[Math.toIntExact(offsets[rows - 1])];
            Object[] values = new Object[Math.toIntExact(offsets[rows - 1])];

            for (int i = 0; i < offsets[rows - 1]; i++) {
                keys[i] = keyType.deserializeBinary(deserializer);
            }

            for (int i = 0; i < offsets[rows - 1]; i++) {
                values[i] = valueType.deserializeBinary(deserializer);
            }

            long start = 0l;
            for (int i = 0; i < rows; i++) {
                Map map = new HashMap();
                for (long j = start; j < offsets[i]; j++) {
                    map.put(keys[(int) j],values[(int) j]);
                }
                data[i] = map;
                start = offsets[i];
            }
        }
        return data;
    }

    public static IDataType createMapType(SQLLexer lexer, PhysicalInfo.ServerInfo serverInfo) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        IDataType keyType = DataTypeFactory.get(lexer, serverInfo);
        Validate.isTrue(lexer.character() == ',');
        IDataType valueType = DataTypeFactory.get(lexer, serverInfo);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeMap("Map(" + keyType.name() + ", " + valueType.name() + ")",
                keyType, valueType);
    }
}
