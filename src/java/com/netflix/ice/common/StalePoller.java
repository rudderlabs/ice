package com.netflix.ice.common;

public abstract class StalePoller extends Poller {
    private boolean stale = true;
    public final int DefaultStalePollIntervalSecs = 5 * 60;

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

	/**
	 * 
	 * @return Return true if data is no longer stale
	 * @throws Exception
	 */
    protected abstract boolean stalePoll() throws Exception;
}
