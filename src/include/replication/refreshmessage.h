/*-------------------------------------------------------------------------
 * refreshmessage.h
 *	   Exports from replication/logical/refreshmessage.c
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * src/include/replication/refreshmessage.h
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_REFRESH_MESSAGE_H
#define PG_LOGICAL_REFRESH_MESSAGE_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"

/*
 * Generic logical decoding refresh message wal record.
 */
typedef struct xl_logical_refresh_message
{
	Oid			dbId;			/* database Oid emitted from */
	Size		prefix_size;	/* length of prefix */
	Size		role_size;      /* length of the role that executes the refresh command */
	Size		search_path_size; /* length of the search path */
	Size		message_size;	  /* size of the message */
	/*
	 * payload, including null-terminated prefix of length prefix_size
	 * and null-terminated role of length role_size
	 * and null-terminated search_path of length search_path_size
	 */
	char		message[FLEXIBLE_ARRAY_MEMBER];
} xl_logical_refresh_message;

#define SizeOfLogicalRefreshMessage	(offsetof(xl_logical_refresh_message, message))

extern XLogRecPtr LogLogicalRefreshMessage(const char *prefix, Oid roleoid, const char *refresh_message,
									   size_t size);

/* RMGR API*/
#define XLOG_LOGICAL_REFRESH_MESSAGE	0x00
void		logicalrefreshmsg_redo(XLogReaderState *record);
void		logicalrefreshmsg_desc(StringInfo buf, XLogReaderState *record);
const char *logicalrefreshmsg_identify(uint8 info);

#endif							/* PG_LOGICAL_REFRESH_MESSAGE_H */
