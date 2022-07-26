/*-------------------------------------------------------------------------
 * refreshdata.h
 *	   Exports from replication/logical/refreshdata.c
 *     This header/source is separated from refreshmessage to allow
 *     for additional logic to support refreshing data without 
 *     altering the DDL-analogous design of the refresh message 
 *     record type.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * src/include/replication/refreshdata.h
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_REFRESH_DATA_H
#define PG_LOGICAL_REFRESH_DATA_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "executor/tuptable.h"

#define XLL_REFRESH_CONCURR			(1<<0)
#define XLL_REFRESH_SKIPDATA		(1<<1)
#define XLL_REFRESH_COMPLETEQUERY	(1<<2)

/*
 * Generic logical decoding refresh message wal record.
 */
typedef struct xl_logical_refresh_data
{
	Oid			dbId;			/* database Oid emitted from */
	Oid			matviewId;

	uint8		flags; /* boolean flags on refresh */

	Size		prefix_size;	/* length of prefix */
	Size		message_size;	  /* size of the message */

	char		message[FLEXIBLE_ARRAY_MEMBER];
} xl_logical_refresh_data;

#define SizeOfLogicalRefreshData	(offsetof(xl_logical_refresh_data, message))

extern XLogRecPtr LogLogicalRefreshData(
	const char *prefix, const char *message, Oid matviewOid,
	bool concurrent, bool skipData, bool isCompleteQuery);

/* RMGR API*/
#define XLOG_LOGICAL_REFRESH_DATA	0x00
void		logicalrefreshdata_redo(XLogReaderState *record);
void		logicalrefreshdata_desc(StringInfo buf, XLogReaderState *record);
const char *logicalrefreshdata_identify(uint8 info);

#endif							/* PG_LOGICAL_REFRESH_MESSAGE_H */
