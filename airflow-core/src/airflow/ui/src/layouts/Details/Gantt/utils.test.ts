/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { describe, expect, it } from "vitest";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import type { GridTask } from "src/layouts/Details/Grid/utils";

import {
  type GanttDataItem,
  buildGanttRowSegments,
  buildGanttTimeAxisTicks,
  GANTT_TIME_AXIS_TICK_COUNT,
  gridSummariesToTaskIdMap,
} from "./utils";

describe("buildGanttTimeAxisTicks", () => {
  it("returns evenly spaced elapsed labels with edge alignment", () => {
    const minMs = 0;
    const maxMs = 60_000;
    const ticks = buildGanttTimeAxisTicks(minMs, maxMs);

    expect(ticks).toHaveLength(GANTT_TIME_AXIS_TICK_COUNT);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.label).toBe("00:00:00");
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.leftPct).toBe(100);
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.labelAlign).toBe("right");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.label).toBe("00:01:00");
    expect(ticks[1]?.labelAlign).toBe("center");
    expect(ticks.every((tick) => typeof tick.label === "string" && tick.label.length > 0)).toBe(true);
  });

  it("supports a single tick", () => {
    const ticks = buildGanttTimeAxisTicks(1000, 1000, 1);

    expect(ticks).toHaveLength(1);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[0]?.label).toBe("00:00:00");
  });
});

describe("gridSummariesToTaskIdMap", () => {
  it("indexes summaries by task_id", () => {
    const summaries = [
      { state: null, task_id: "a" } as LightGridTaskInstanceSummary,
      { state: null, task_id: "b" } as LightGridTaskInstanceSummary,
    ];
    const map = gridSummariesToTaskIdMap(summaries);

    expect(map.get("a")).toBe(summaries[0]);
    expect(map.get("b")).toBe(summaries[1]);
    expect(map.size).toBe(2);
  });
});

describe("buildGanttRowSegments", () => {
  it("groups items by task id in flat node order", () => {
    const flatNodes: Array<GridTask> = [
      { depth: 0, id: "t1", is_mapped: false, label: "a" } as GridTask,
      { depth: 0, id: "t2", is_mapped: false, label: "b" } as GridTask,
    ];
    const items: Array<GanttDataItem> = [
      { taskId: "t2", x: [1_577_836_800_000, 1_577_923_200_000], y: "b" },
      { taskId: "t1", x: [1_577_836_800_000, 1_577_923_200_000], y: "a" },
    ];

    const segments = buildGanttRowSegments(flatNodes, items);

    expect(segments).toHaveLength(2);
    expect(segments[0]?.map((segment) => segment.taskId)).toEqual(["t1"]);
    expect(segments[1]?.map((segment) => segment.taskId)).toEqual(["t2"]);
  });
});
