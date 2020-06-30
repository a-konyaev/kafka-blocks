package kafkablocks.consumer;

/**
 * Handler for events of playback time changing
 */
public interface PlaybackTimeHandler {
    /**
     * Playback time was changed (moved on)
     * @param timestamp current playback timestamp
     */
    void onPlaybackTimeChanged(long timestamp);
}
