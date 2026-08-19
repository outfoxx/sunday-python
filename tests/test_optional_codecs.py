# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import pytest

from sunday import MediaType
from sunday.xml import XmlCodec
from sunday.yaml import YamlCodec


def test_yaml_codec_matches_structured_suffixes() -> None:
    codec = YamlCodec()
    encoded = codec.encode({"project": {"name": "Roadmap"}})

    assert codec.decode(encoded, MediaType("application/vnd.project+yaml")) == {"project": {"name": "Roadmap"}}
    assert any(media_type.matches(MediaType("application/vnd.project+yaml")) for media_type in codec.media_types)


def test_xml_codec_requires_one_root_and_matches_structured_suffixes() -> None:
    codec = XmlCodec()
    encoded = codec.encode({"project": {"name": "Roadmap"}})

    assert codec.decode(encoded, MediaType("application/vnd.project+xml")) == {"project": {"name": "Roadmap"}}
    assert any(media_type.matches(MediaType("application/vnd.project+xml")) for media_type in codec.media_types)
    with pytest.raises(TypeError):
        codec.encode({"one": 1, "two": 2})
