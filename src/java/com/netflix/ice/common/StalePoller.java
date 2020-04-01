/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.common;

public abstract class StalePoller extends Poller {
    private boolean stale = true;
    public final int DefaultStalePollIntervalSecs = 1 * 60;

    public void stale() {
    	stale = true;
    }

	@Override
    public void start() {
		// Default to 5 minutes
        start(DefaultStalePollIntervalSecs);
    }

	@Override
	protected void poll() throws Exception {
    	if (!stale)
    		return;
    	stale = stalePoll();    	
	}
	
	public boolean getStale() {
		return stale;
	}

	/**
	 * 
	 * @return Return true if data is no longer stale
	 * @throws Exception
	 */
    protected abstract boolean stalePoll() throws Exception;
}
