#ifndef NANOMQ_ACL_HANDLER_H
#define NANOMQ_ACL_HANDLER_H

#include "nng/nng.h"
#include "nng/supplemental/nanolib/conf.h"
#include "nng/supplemental/nanolib/acl_conf.h"

#ifdef ACL_SUPP
extern bool auth_acl(
    conf *config, acl_action_type type, conn_param *param, const char *topic);

// nmq_acl_lock_init allocates the rwlock that guards config->acl. It must be
// called exactly once from the broker startup path, before any auth_acl or
// reload_acl_config call, so the hot path never races the reload.
extern void nmq_acl_lock_init(void);

// reload_acl_config atomically replaces the live ACL (cur) with a freshly
// parsed one (new) under the ACL write lock. The rules array is moved out of
// new (its rules/rule_count are cleared) so the caller's conf_fini does not
// double-free the transferred rules.
extern void reload_acl_config(conf_acl *cur, conf_acl *new);
#endif
#endif
