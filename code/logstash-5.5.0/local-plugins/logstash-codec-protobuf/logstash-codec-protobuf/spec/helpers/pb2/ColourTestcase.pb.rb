#!/usr/bin/env ruby
# Generated by the protocol buffer compiler. DO NOT EDIT!

require 'protocol_buffers'

# forward declarations
class ColourProtoTest < ::ProtocolBuffers::Message; end

class ColourProtoTest < ::ProtocolBuffers::Message
  # forward declarations

  # enums
  module Colour
    include ::ProtocolBuffers::Enum

    set_fully_qualified_name "ColourProtoTest.Colour"

    BLACK = 0
    BLUE = 1
    WHITE = 2
    GREEN = 3
    RED = 4
    YELLOW = 5
    AQUA = 6
  end

  set_fully_qualified_name "ColourProtoTest"

  repeated ::ColourProtoTest::Colour, :favourite_colours, 1
  repeated :bool, :booleantest, 2
  optional ::ColourProtoTest::Colour, :least_liked, 3
  optional :string, :timestamp, 4
  optional :string, :version, 5
end
