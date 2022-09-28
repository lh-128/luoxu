import logging

from ctxvars import msg_source
from util import UpdateDirection

logger = logging.getLogger('luoxu')

class GroupHistoryIndexer:
  entity = None

  def __init__(self, entity, group_info):
    self.group_id = entity.id
    self.entity = entity
    self.group_info = group_info

  async def run(self, client, dbstore, callback):
    msg_source.set('history')
    group_info = self.group_info
    if group_info['loaded_last_id'] is None:
      first_id = 0
      msgs = await client.get_messages(self.entity, limit=2)
      logger.info('Trying to fetch 2 messages. Received %s message(s) for group %s', len(msgs), self.group_info['title'])
      last_id = msgs[-1].id
    else:
      first_id = self.group_info['loaded_first_id']
      last_id = self.group_info['loaded_last_id']

    # getting newer message since last sync
    while True:
      logger.info('Getting up to 50 new messages form group %s since last_id [%s].', self.group_info['title'], last_id)
      msgs = await client.get_messages(
        self.entity,
        limit = 50,
        # going from the oldest (limited by last_id) to the most recent
        reverse = True,
        min_id = last_id,
      )
      if not msgs:
        break

      if not first_id:
        logger.debug('First ID does not exist, set to bi-directional')
        update_direction = UpdateDirection.update_bidirectional
        first_id = msgs[0].id
      else:
        logger.debug('First ID exists, set to forward updating')
        update_direction = UpdateDirection.update_forward
        last_id = msgs[-1].id

      logger.info('Received %s messages from group %s between [%s] and [%s]', len(msgs), self.group_info['title'], msgs[0].id, msgs[-1].id)
      loaded = await dbstore.insert_messages(msgs, update_direction)
      logger.info('Processed %s actual messages', loaded)
    logger.info('Forward history index done form group %s', self.group_info['title'])
    callback()

    # going older message before first msg id
    if first_id == 1:
      logger.info('First message is already 1. No need to go back for group %s.', self.group_info['title'])
      return

    while True:
      logger.info('Getting up to 50 new messages for group %s before first_id [%s].', self.group_info['title'], first_id)
      msgs = await client.get_messages(
        self.entity,
        limit = 50,
        # from current (or latest) to older
        max_id = first_id,
      )
      if not msgs:
        break

      #reverse order so the list becomes chronological
      msgs = msgs[::-1]
      first_id = msgs[0].id

      logger.info('Received %s messages from group %s between [%s] and [%s]', len(msgs), self.group_info['title'], msgs[0].id, msgs[-1].id)
      loaded = await dbstore.insert_messages(msgs, UpdateDirection.update_backward)
      logger.info('Processed %s actual messages', loaded)

    logger.info('backward history index done for group %s', self.group_info['title'])