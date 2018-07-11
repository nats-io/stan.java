// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.streaming;

import static org.junit.Assert.assertEquals;

import java.time.Duration;

import org.junit.Test;

public class OptionsTests {
    @Test
    public void testBuilderFromTempalte() {
        Options opts = new Options.Builder().
                        maxPubAcksInFlight(10).
                        natsUrl("nats://superserver:4222").
                        pubAckWait(Duration.ofMillis(1000)).
                        build();
        Options opts2 = new Options.Builder(opts).build();

        assertEquals(opts.getMaxPubAcksInFlight(), opts2.getMaxPubAcksInFlight());
        assertEquals(opts.getNatsUrl(), opts2.getNatsUrl());
        assertEquals(opts.getAckTimeout(), opts2.getAckTimeout());
    }
}