from typing import NamedTuple, Optional
import datetime

class SearchCriteria(NamedTuple):
  group_id: int
  terms: Optional[str]
  sender: Optional[str]
  start: Optional[datetime.datetime]
  end: Optional[datetime.datetime]

class GroupNotFound(Exception):
  def __init__(self, group):
    self.group = group

  def __str__(self):
    return f'no such group indexed: {self.group}'
