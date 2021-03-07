INSERT INTO login_tokens (user_id, uuid, time_created)
VALUES (%(user_id)s, %(uuid)s, %(time)s)
ON DUPLICATE KEY UPDATE
  user_id = %(user_id)s
  uuid = %(uuid)s
  time_created = %(time)s