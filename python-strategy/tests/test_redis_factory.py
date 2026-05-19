"""Tests for src/core/redis_factory.py — centralized Redis client factory."""

from unittest.mock import patch, MagicMock

from src.core.redis_factory import create_redis_client


class TestCreateRedisClient:

    @patch("src.core.redis_factory.redis.Redis")
    def test_default_config_no_password(self, mock_redis_cls):
        """Default call should use localhost:6379, decode_responses, no password."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {}, clear=True):
            create_redis_client()

        mock_redis_cls.assert_called_once_with(
            host="localhost",
            port=6379,
            decode_responses=True,
        )

    @patch("src.core.redis_factory.redis.Redis")
    def test_with_redis_password(self, mock_redis_cls):
        """REDIS_PASSWORD env var should be passed as password kwarg."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {"REDIS_PASSWORD": "s3cret"}, clear=True):
            create_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["password"] == "s3cret"

    @patch("src.core.redis_factory.redis.Redis")
    def test_empty_password_treated_as_no_auth(self, mock_redis_cls):
        """Empty REDIS_PASSWORD should not add password kwarg."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {"REDIS_PASSWORD": ""}, clear=True):
            create_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert "password" not in call_kwargs

    @patch("src.core.redis_factory.redis.Redis")
    def test_custom_kwargs_override_defaults(self, mock_redis_cls):
        """Caller kwargs should override defaults."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {}, clear=True):
            create_redis_client(host="redis.prod", port=6380, db=2)

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["host"] == "redis.prod"
        assert call_kwargs["port"] == 6380
        assert call_kwargs["db"] == 2

    @patch("src.core.redis_factory.redis.Redis")
    def test_host_from_env(self, mock_redis_cls):
        """REDIS_HOST env var should be used."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {"REDIS_HOST": "redis-server"}, clear=True):
            create_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["host"] == "redis-server"

    @patch("src.core.redis_factory.redis.Redis")
    def test_port_from_env(self, mock_redis_cls):
        """REDIS_PORT env var should be used."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {"REDIS_PORT": "6380"}, clear=True):
            create_redis_client()

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["port"] == 6380

    @patch("src.core.redis_factory.redis.Redis")
    def test_returns_redis_instance(self, mock_redis_cls):
        """Should return the redis.Redis instance."""
        sentinel = MagicMock()
        mock_redis_cls.return_value = sentinel

        with patch.dict("os.environ", {}, clear=True):
            result = create_redis_client()

        assert result is sentinel

    @patch("src.core.redis_factory.redis.Redis")
    def test_decode_responses_can_be_overridden(self, mock_redis_cls):
        """Caller should be able to disable decode_responses."""
        mock_redis_cls.return_value = MagicMock()

        with patch.dict("os.environ", {}, clear=True):
            create_redis_client(decode_responses=False)

        call_kwargs = mock_redis_cls.call_args[1]
        assert call_kwargs["decode_responses"] is False
