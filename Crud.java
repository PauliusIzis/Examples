package main.Services.Impl;

import javafx.util.Pair;
import main.Models.BL.DBQueryModel;
import main.Models.BL.ProcedureModel;
import main.Models.BL.UpdateModel;
import main.Models.BL.UpdatePreparedStmtModel;
import main.Models.DAL.UserExtendedDAL;
import main.Models.DTO.DBqueryDTO;
import main.Services.Helpers.Logger;
import main.Services.Helpers.QueryBuilder;
import main.Services.ICrud;
import main.Services.IDataBase;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Crud implements ICrud {

    @Override
    public DBqueryDTO create(Object object) {
        IDataBase dataBase = new DataBase();
        Connection connection = dataBase.getConnection();
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement(createInsertQuery(object));

            // Exception for inserting Images (byteArray) into DB from 'UserExtendedDAL'
            if (object.getClass().getSimpleName().equals("UserExtendedDAL")) {
                UserExtendedDAL userExtendedDAL = (UserExtendedDAL) object;
                preparedStatement.setBytes(1, userExtendedDAL.profileImg);
            }

            int rowsCreated = preparedStatement.executeUpdate();
            preparedStatement.close();
            return new DBqueryDTO<>(true, "Rows created -> " + rowsCreated, null);
        } catch (Exception e) {
            Logger.error(e.getMessage());
            e.printStackTrace();
            return new DBqueryDTO<>(false, e.getMessage(), null);
        } finally {
            dataBase.closeConnection(connection);
        }
    }

    @Override
    public <T> DBqueryDTO<T> read(DBQueryModel dbQueryModel, Class<T> dalType) {
        IDataBase dataBase = new DataBase();
        Connection connection = dataBase.getConnection();
        try {
            List<T> rows = getDALList(connection, dbQueryModel, dalType);
            return new DBqueryDTO<>(true, "Rows selected from DB -> " + rows.size(), rows);
        } catch (Exception e) {
            Logger.error(e.getMessage());
            e.printStackTrace();
            return new DBqueryDTO<>(false, e.getMessage(), null);
        } finally {
            dataBase.closeConnection(connection);
        }
    }

    @Override
    public DBqueryDTO update(Object dal, String[] primaryKey) {
        IDataBase dataBase = new DataBase();
        Connection connection = dataBase.getConnection();
        try {
            UpdatePreparedStmtModel preparedStmtModel = new UpdatePreparedStmtModel();
            preparedStmtModel.connection = connection;
            preparedStmtModel.dalWithUpdatedValues = dal;
            preparedStmtModel.primaryKeys = primaryKey;
            preparedStmtModel.tableName = getClassNameWithoutDAL(dal.getClass());

            PreparedStatement preparedStatement = createPreparedStatementForUpdate(preparedStmtModel);
            int rowsUpdated = preparedStatement.executeUpdate();
            preparedStatement.close();
            return new DBqueryDTO<>(true, "Rows updated -> " + rowsUpdated, null);
        } catch (Exception e) {
            Logger.error(e.getMessage());
            e.printStackTrace();
            return new DBqueryDTO<>(false, e.getMessage(), null);
        } finally {
            dataBase.closeConnection(connection);
        }
    }

    @Override
    public DBqueryDTO delete(DBQueryModel deleteModel, Class dal) {
        IDataBase dataBase = new DataBase();
        Connection connection = dataBase.getConnection();
        try {
            Statement statement = connection.createStatement();
            int rowsDeleted = statement.executeUpdate(QueryBuilder
                                        .buildQuery(getClassNameWithoutDAL(dal), deleteModel, "delete"));
            statement.close();
            return new DBqueryDTO<>(true, "Rows deleted -> " + rowsDeleted, null);
        } catch (Exception e) {
            Logger.error(e.getMessage());
            e.printStackTrace();
            return new DBqueryDTO<>(false, e.getMessage(), null);
        } finally {
            dataBase.closeConnection(connection);
        }
    }

    private boolean checkForPrimaryKey(String fieldName, String[] primaryKeyArr){
        for (String pk : primaryKeyArr) {
            if (pk.equals(fieldName)) return true;
        }
        return false;
    }

    private Class<?> boxPrimitiveClass(Class<?> type) {
        if (type == int.class) {
            return Integer.class;
        } else if (type == long.class) {
            return Long.class;
        } else if (type == double.class) {
            return Double.class;
        } else if (type == float.class) {
            return Float.class;
        } else if (type == boolean.class) {
            return Boolean.class;
        } else if (type == byte.class) {
            return Byte.class;
        } else if (type == char.class) {
            return Character.class;
        } else if (type == short.class) {
            return Short.class;
        } else {
            // For now this method only works with primitive Data types in other cases it should be expanded
            throw new IllegalArgumentException("Class '" + type.getName() + "' is not a primitive");
        }
    }

    private String createInsertQuery(Object object)
            throws IllegalArgumentException, IllegalAccessException {
        Class<?> zclass = object.getClass();
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(getClassNameWithoutDAL(zclass))
                .append(" VALUES (");
        Field[] fields = zclass.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            fields[i].setAccessible(true);

            // During 'Create' 'UserDAL's field 'userId' should be skipped, DB automatically assigns a unique ID once inserted
            if (fields[i].getName().equals("userId") && object.getClass().getSimpleName().equals("UserDAL")) {
                continue;
                // In case we try to insert 'UserExtendedDAL' we have to properly handle Img byte array
                // In this case we will set this value as '?' and insert the byte array later with appropriate methods
            } else if (fields[i].getName().equals("profileImg")) {
                sb.append("?");
                // In case given Objects field is not initialized we set it to 'null'
            } else if (fields[i].get(object) == null) {
                sb.append("null");
            } else {
                // All other cases get their field value assigned
                sb.append(quoteIdentifier(fields[i].get(object).toString()));
            }

            // If this is not the last field we append ',' otherwise we add ')' to close the VALUES clause
            if (i != fields.length - 1) {
                sb.append(",");
            } else {
                sb.append(")");
            }

        }
        return sb.toString();
    }

    private PreparedStatement createPreparedStatementForUpdate(UpdatePreparedStmtModel preparedStmtModel) throws Exception {
        Class<?> zclass = preparedStmtModel.dalWithUpdatedValues.getClass();
        // First we create UpdateModel which will have the SQL query and which values should be in the 'WHERE' clause
        UpdateModel updateModel = createUpdateQuery(zclass, preparedStmtModel.tableName, preparedStmtModel.primaryKeys);
        // We crate a PreparedStatement with a query with '?' instead of values -> "UPDATE ... 'userId' = ?, ..."
        PreparedStatement stmt = preparedStmtModel.connection.prepareStatement(updateModel.query);
        int fieldPosition = 1;
        for (Field field : zclass.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(preparedStmtModel.dalWithUpdatedValues);
            String name = field.getName();

            if (checkForPrimaryKey(name, preparedStmtModel.primaryKeys)) {
                // Get current PrimaryKey position from 'updateModel' arrayLists
                int pkPosition = updateModel.pkLocation.get(updateModel.primaryKeys.indexOf(name));
                // Set that Fields value into the PreparedStatement at given 'pkPosition'
                stmt.setObject(pkPosition, value);
            } else {
                // Not a primary key so it should be set at the current field position
                stmt.setObject(fieldPosition, value);
                fieldPosition++;
            }

        }
        return stmt;
    }

    private UpdateModel createUpdateQuery(Class<?> zclass, String tableName, String[] primaryKey) {
        StringBuilder sets = new StringBuilder();
        UpdateModel updateModel = new UpdateModel();
        updateModel.pkLocation = new ArrayList<>();
        updateModel.primaryKeys = new ArrayList<>();

        String where = null;
        Field[] fields = zclass.getDeclaredFields();
        // This is the starting position for the 'WHERE' clause
        // In SQL statements numeration starts at 1 -> "UPDATE ... SET ?, ? WHERE ?" the '?' would go from 1 to 3
        // This position is calculated by taking the Object field count subtracting number of primaryKeys (where clauses)
        // and adding 1 (in case there is only 1 primary key)
        int pkPosition = fields.length - primaryKey.length + 1;
        for (Field field : fields) {
            String name = field.getName();
            String pair = name + " = ?";
            // First we check if the current Field is one of the primaryKeys
            if (checkForPrimaryKey(name, primaryKey)) {
                // If this is the first encountered primaryKey
                if (updateModel.primaryKeys.isEmpty()) {
                    where = " WHERE " + pair;
                } else {
                    where += " AND " + pair;
                }
                // Add the current primaryKey location and its name to a separate ArrayList
                updateModel.pkLocation.add(pkPosition);
                updateModel.primaryKeys.add(name);
                // Increment PK position for next possible PK
                pkPosition++;

            } else {
                // Appends ', ' if this is NOT the first pair
                if (sets.length() > 1) {
                    sets.append(", ");
                }
                sets.append(pair);

            }
        }
        // In case there were no given primaryKeys throw an exception
        if (where == null) {
            throw new IllegalArgumentException("Primary key not found in '" + zclass.getName() + "'");
        }

        updateModel.query = "UPDATE " + tableName + " SET " + sets.toString() + where;
        return updateModel;
    }

    private CallableStatement getCallableStatementForGivenProcedure(Connection connection, ProcedureModel procedure) throws Exception {
        // We prepare the Statement by using the given Procedure name (check Procedures class for more info).
        CallableStatement callableStatement = connection.prepareCall(procedure.name);
        // Procedures might require a number of arguments which consist of (String) ArgName and (?) Value
        // For that Procedure class has a List of Pair<String, Object>
        List<Pair<String, Object>> params = procedure.params;
        for (Pair<String, Object> pair : params) {
            // We check what kind of Data type the value is
            // For now we only check for two values: 'Integer' and 'String' because we do not need other values yet
            switch (pair.getValue().getClass().getSimpleName()) {
                case "Integer":
                    callableStatement.setInt(pair.getKey(), (int) pair.getValue());
                    break;
                case "String":
                    callableStatement.setString(pair.getKey(), (String) pair.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("'getCallableStatementForGivenProcedure' method can only handle " +
                            "'Integer' and 'String' types for procedure arguments.");
            }
        }
        return callableStatement;
    }

    private String getClassNameWithoutDAL(Class c) {
        // Exception: 'User' is a keyword in SQL because of that we must use []
        if (c.getSimpleName().equals("UserDAL")) {
            return "[User]";
        }
        return c.getSimpleName().replace("DAL", "");
    }

    private <T> List<T> getDALList(Connection connection, DBQueryModel queryModel, Class<T> dalType) throws Exception {
        ResultSet rs;
        CallableStatement callableStatement;
        Statement statement;
        List<T> rows;

        // First we check if 'Read' was called with a Procedure for which we need to use CallableStatement
        if (queryModel.procedure != null) {
            callableStatement = getCallableStatementForGivenProcedure(connection, queryModel.procedure);
            callableStatement.execute();
            rs = callableStatement.getResultSet();
            rows = getDALListFromResultSet(rs, dalType);
            callableStatement.close();
        } else {
            // In this case the call to 'Read' was made as a regular query (without a procedure)
            statement = connection.createStatement();
            rs = statement.executeQuery(QueryBuilder
                            .buildQuery(getClassNameWithoutDAL(dalType), queryModel, "read"));
            rows = getDALListFromResultSet(rs, dalType);
            statement.close();
        }

        return rows;
    }

    private <T> List<T> getDALListFromResultSet(ResultSet rs, Class<T> dalType) throws Exception {
        List<T> rows = new ArrayList<>();
        while (rs.next()) {
            // During each 'while' iteration creates a new Object of the given Class <T>
            T dal = dalType.newInstance();
            loadResultSetIntoObject(rs, dal);
            rows.add(dal);
        }
        rs.close();
        return rows;
    }


    private void loadResultSetIntoObject(ResultSet rst, Object object)
            throws IllegalArgumentException, IllegalAccessException, SQLException {
        Class<?> zclass = object.getClass();
        for (Field field : zclass.getDeclaredFields()) {
            String name = field.getName();
            field.setAccessible(true);
            // We get the value from ResultSet for current Field and set it to Object type for now
            Object value = rst.getObject(name);
            // We get the Data type for the current Field
            Class<?> type = field.getType();

            if (isPrimitive(type)) {
                // Returns an equivalent Class for given primitive Data type
                // For example: if type == 'int' -> boxed would be assigned 'Integer' class
                Class<?> boxed = boxPrimitiveClass(type);
                // Exception: SQL doesn't have 'boolean' data type, we need to check it's 'int' value (0 or 1)
                if (type == boolean.class) {
                    value = (int) value == 1;
                // Exception: SQL doesn't have 'long' Data type, so we need to parse it's 'String' value
                } else if (type == long.class) {
                    value = Long.parseLong(value.toString());
                } else {
                // All other cases get the ResultSet value cast as their AutoBox class
                    value = boxed.cast(value);
                }

            }
            field.set(object, value);
        }
    }

    private String quoteIdentifier(String value) {
        return "'" + value + "'";
    }

    private boolean isPrimitive(Class<?> type) {
        return (type == int.class || type == long.class ||
                type == double.class || type == float.class
                || type == boolean.class || type == byte.class
                || type == char.class || type == short.class);
    }
}


