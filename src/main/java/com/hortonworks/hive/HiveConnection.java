///*
// * Copyright 2015 artem.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.hortonworks.hive;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.logging.Logger;
//
///**
// *
// * @author artem
// */
//public class HiveConnection {
//
//    private static Connection connection = null;
//    private static final Logger LOG = Logger.getLogger(HiveConnection.class.getName());
//
//    public static Connection getHiveConnection(String urlString, String database,
//            String user, String password) {
//        try {
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
//            connection = DriverManager.getConnection("jdbc:hive2://" + urlString + ":10000/" + database, user, password);
//            //Connection con = DriverManager.getConnection("jdbc:hive2://ec2-54-174-248-248.compute-1.amazonaws.com:10000/default", "", "");
//        } catch (ClassNotFoundException ex) {
//            throw new RuntimeException("Hive Driver class not found");
//        } catch (SQLException ex) {
//            throw new RuntimeException("error getting connection to Hive");
//        }
//        return connection;
//    }
//}
