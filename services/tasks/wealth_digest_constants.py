"""ak-ran v2 #5: single-source-of-truth constants for the WealthDigest
task's routing identifiers.

Why a separate leaf module: the job-title string is duplicated across
three writer sites (scheduler.TASK_MAPPING key, InvestmentService.
jobsObject key, bootstrap_wealth_digest.py _JOB_TITLE) plus one
reader site (WealthDigestTask itself). A typo at any of those sites
silently breaks routing — scheduler._get_task_class returns None,
the Job never fires, the digest never lands, and there's no error
to grep for.

This module has ZERO imports beyond the standard library so it can
be imported from anywhere in the codebase — including InvestmentService
which would otherwise create an import cycle with services.tasks.*
(BaseTask -> InvestmentService -> WealthDigestTask -> BaseTask). Keep
it leaf-only forever.
"""

# The scheduler's routing key for the WealthDigest task. This is the
# `title` on the Job row, the key in scheduler.TASK_MAPPING, the key
# in InvestmentService.jobsObject, and what bootstrap_wealth_digest.py
# writes as Job.title. All four sites import this constant so a
# typo becomes a compile-time NameError instead of a silent routing
# miss.
#
# Distinct from services.agentConversationService.WEALTH_DIGEST_TITLE
# which is the conversation TITLE (user-facing string "Wealth Digest").
# Kept as two separate constants because the two concepts can diverge
# (Phase 2 might change the conversation title without changing the
# job title, or vice versa).
WEALTH_DIGEST_JOB_TITLE = "WealthDigest"
