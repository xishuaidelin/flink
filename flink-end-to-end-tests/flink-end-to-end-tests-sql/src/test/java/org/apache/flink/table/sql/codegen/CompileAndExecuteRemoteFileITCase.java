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

import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** End-to-End tests for compile and execute remote file. */
public class CompileAndExecuteRemoteFileITCase extends RemoteITCaseBase {

    public CompileAndExecuteRemoteFileITCase() {
        super("streaming");
    }

    @Before
    public void before() throws Exception {
        super.before();
        createHDFS();
    }

    private void createHDFS() {
        Configuration hdConf = configureHDFS();
        try {
            hdPath = new org.apache.hadoop.fs.Path("/test.json");
            hdfs = hdPath.getFileSystem(hdConf);
        } catch (Throwable e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Override
    protected Map<String, String> generateReplaceVars() {
        String remoteJsonPath =
                String.format(
                        "hdfs://%s:%s/%s",
                        hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort(), hdPath);

        Map<String, String> map = super.generateReplaceVars();
        map.put("$HDFS_Json_Plan_PATH", remoteJsonPath);
        return map;
    }

    @Test
    public void testCompilePlanRemoteFile() throws Exception {
        runAndCheckSQL("compile_plan_use_remote_file_e2e.sql", generateReplaceVars(), 0, null);
    }

    @Override
    protected void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws Exception {
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).build(),
                Duration.ofMinutes(2L));
    }
}
