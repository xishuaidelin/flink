/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sql.codegen;

import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** End to End Testbase for using remote jar and compile and execute remote file. */
public class RemoteITCaseBase extends SqlITCaseBase {
    protected static final Path HADOOP_CLASSPATH =
            ResourceTestUtils.getResource(".*hadoop.classpath");

    protected MiniDFSCluster hdfsCluster;
    protected org.apache.hadoop.fs.Path hdPath;
    protected org.apache.hadoop.fs.FileSystem hdfs;

    public RemoteITCaseBase(String executionMode) {
        super(executionMode);
    }

    @BeforeClass
    public static void verifyOS() {
        Assume.assumeTrue(
                "HDFS cluster cannot be started on Windows without extensions.",
                !OperatingSystem.isWindows());
    }

    @Before
    public void before() throws Exception {
        super.before();
    }

    public Configuration configureHDFS() {
        try {
            Configuration hdConf = new Configuration();
            File baseDir = new File("./target/hdfs/hdfsTest").getAbsoluteFile();
            FileUtil.fullyDelete(baseDir);
            hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
            hdfsCluster = builder.build();
            return hdConf;
        } catch (Throwable e) {
            Assert.fail("Test failed " + e.getMessage());
        }
        return null;
    }

    @After
    public void destroyHDFS() {
        try {
            hdfs.delete(hdPath, false);
            hdfsCluster.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws Exception {}
}
