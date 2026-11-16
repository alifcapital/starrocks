// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.scheduler;

import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.LogUtil;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// The status of every run shows the definition of its task on one line; the task makes it once per definition.
public class TaskDefinitionForStatusTest {

    private static String expected(String definition) {
        return MvUtils.shrinkToSize(LogUtil.removeLineSeparator(definition), SystemTable.MAX_FIELD_VARCHAR_LENGTH - 1);
    }

    @Test
    public void definitionForStatusIsMadeOncePerDefinition() {
        Task task = new Task("t");
        Assertions.assertNull(task.getDefinitionForStatus());
        task.setDefinition("");
        Assertions.assertNull(task.getDefinitionForStatus());

        String definition = "insert into t\nselect 'a\nb', -- comment\n 1\r\nfrom s";
        task.setDefinition(definition);
        String shown = task.getDefinitionForStatus();
        Assertions.assertEquals(expected(definition), shown);
        Assertions.assertSame(shown, task.getDefinitionForStatus());

        String other = "select 2\nfrom u";
        task.setDefinition(other);
        Assertions.assertEquals(expected(other), task.getDefinitionForStatus());

        String longDefinition = "select 1 -- x\n".repeat(SystemTable.MAX_FIELD_VARCHAR_LENGTH / 4);
        task.setDefinition(longDefinition);
        Assertions.assertEquals(expected(longDefinition), task.getDefinitionForStatus());

        TaskRun run = TaskRunBuilder.newBuilder(task).build();
        Assertions.assertEquals(expected(longDefinition), run.initStatus("q", 1L).getDefinition());
    }
}
