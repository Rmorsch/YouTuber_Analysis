from youtuber_analysis.assets.raw_youtube import load_channel_config


def test_channels_config_has_required_keys():
    channels = load_channel_config()
    assert channels, "channels.yaml should list at least one channel"
    for channel in channels:
        assert "channel_id" in channel
        assert "channel_name" in channel
        assert channel.get("phase") in (1, 2)
