/*-------------------------------------------------------------------------
 *
 * view.h
 *
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/view.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VIEW_H
#define VIEW_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

typedef struct ViewRecurse_context
{
	Oid			currviewoid;		/* OID of the view */
	List	   *ancestor_views; 	/* OIDs of ancestor views, cleared per full ViewRecurse run */
	List 	   *views;				/* store view oids for view replication - is longer lasting than above */
	List 	   *tables;				/* store table oids for view replication - long lasting */
} ViewRecurse_context;

extern ObjectAddress DefineView(ViewStmt *stmt, const char *queryString,
								int stmt_location, int stmt_len);

extern void StoreViewQuery(Oid viewOid, Query *viewParse, bool replace);
extern void ViewRecurse(ViewRecurse_context *context);

#endif							/* VIEW_H */
