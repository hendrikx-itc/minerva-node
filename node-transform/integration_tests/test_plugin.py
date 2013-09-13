import pkg_resources

from nose.tools import eq_, raises, assert_not_equal, assert_raises, with_setup

from minerva.util import first

from minerva.node import MinervaContext


ENTRYPOINT = "node.plugins"


def load_plugin():
    """
    Load and return the plugin.
    """
    return first([entrypoint.load() for entrypoint in pkg_resources.iter_entry_points(group=ENTRYPOINT, name="transform")])


def test_plugin_loading():
    plugin = load_plugin()

    eq_(plugin.name, "transform")


def test_job_creation():
    """
    Test if method `create_job` on plugin instance works.
    """
    plugin = load_plugin()

    minerva_context = MinervaContext(None, None)

    instance = plugin(minerva_context)

    job_id = -1
    description = {
        "function_set_id": 42,
        "dest_timestamp": "2012-12-11 14:15:00"}
    config = {}

    job = instance.create_job(job_id, description, config)

    assert_not_equal(job, None)
