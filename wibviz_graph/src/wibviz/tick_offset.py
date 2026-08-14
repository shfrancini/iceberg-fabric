class TickOffset:
    slot_id: int
    stream_id: int
    
    def __init__(self, offset, slot_id, stream_id, is_on=False):
        self.offset = offset
        self.slot_id = slot_id
        self.stream_id = stream_id
        self.on = is_on
    
    def is_on(self):
        return self.on
    
    def increment_offset(self, value):
        self.offset += value
        
class TickOffsetHelper:
    on: bool
    
    def __init__(self, on=False):
        self.on = on
        self.tick_offsets = []
    
    def is_on(self):
        return self.on
    
    def get_tick_offset_for_slot_stream(self, slot_id, stream_id):
        for tick_offset in self.tick_offsets:
            if tick_offset.slot_id == slot_id and tick_offset.stream_id == stream_id:
                return tick_offset
        new_tick_offset = TickOffset(offset=0, slot_id=slot_id, stream_id=stream_id)
        self.tick_offsets.append(new_tick_offset)
        return new_tick_offset

    def increment_tick_offset_64_for_slot_stream(self, slot_id, stream_id):
        tick_offset = self.get_tick_offset_for_slot_stream(slot_id, stream_id)
        tick_offset.increment_offset(64)
