"""ak-2r8: BaseRateTask — hoists SetMFRate coverage gates fleet-wide.

Background
==========
Through the MF hardening arc (ak-lp6 → ak-539 → ak-5jq → ak-iwj → ak-nl4), a
coverage-protection layer was built to prevent silent-degradation from
clobbering a good rates file with a near-empty replacement. That layer lived
ONLY inside SetMFRate.run() — the other four rate tasks (SetNPSRate,
SetPPFRate, SetEPFRate, SetIBJAGoldRate) inherited BaseTask's atomic-write /
JSON-validate-before-delete wins from ak-iwj, but NOT the gate logic. A
Pattern-B/C task returning partial-but-valid-JSON would happily
safe_replace_file the near-empty payload and clobber last-good rates, or
worse: return `{"data": []}` and complete with "Completed" status
(silent-green). This is a latent zero-loss gap.

Design
======
BaseRateTask extends BaseTask and provides a concrete `run()` that layers
three coverage gates over subclass-provided fetch results:

  1. Degenerate-answerable guard — if the answerable universe collapses to
     zero (all "permanent skips"), preserve last-good and return Failed.
     Cannot compute success_ratio against an empty universe; treat as a
     transient upstream state, not a "0% success" event.
  2. Hard-floor gate (default 0.5) — if success_ratio falls below the floor,
     skip BOTH the tmp write AND the safe_replace_file swap; the previous
     good rates file stays on disk untouched. Callers keep serving last-good.
     Rationale: an outage-shape result_map is near-empty but size > 0; the
     write pipeline can't distinguish "1 legit rate" from "1 rate + 200 lost
     rates" without a floor. Below the floor → transient-outage-class.
  3. 98% partial-success gate — above the hard floor, the file IS written
     (partial data is better than stale for most callers) but the job status
     returns Failed for operator visibility. Downstream jobs table and dashboards
     see the degraded state instead of a silent-green.

Subclasses implement `_fetch_all()` which returns a 5-tuple describing the
fetch outcome. The base does the rest:

    (data_dict, total, ok, permanent_skips_dict, extras_dict)

  * data_dict            — the payload to write to disk (or None on outright
                           fetch failure).
  * total                — size of the enumerated fetch universe. For AI
                           extract this is 1 (one extraction attempt); for
                           batch fetches like MF this is the URL count.
  * ok                   — successful items (0 <= ok <= total).
  * permanent_skips_dict — {classification_name: count} of items that are
                           excluded from the answerable denominator (e.g.
                           404 delisted schemes for MF; expected empty
                           PFM×scheme combinations for NPS). Also surfaces
                           in the completed-message template.
  * extras_dict          — subclass-specific metadata (error message on
                           fetch failure, template variables for the
                           completed-message override, etc.).

Q-gate decisions (per Lead's dispatch)
======================================
* Q1: gates-only hoist (not the full rate-task lifecycle) — the abstraction
      hoists the gate LOGIC, not error classification / retry loops / fetch
      pipeline. Per-task pipelines stay in each subclass's _fetch_all().
* Q2: NPS uses option (a) — simple ratio with low-but-nonzero thresholds to
      catch the empty-response silent-clobber (0.05 / 0.05) until Phase 2's
      count-deviation baseline lands via follow-up bead
      ak-2r8-fb-nps-count-deviation.
* Q3-Q6: defaults GO. File I/O via BaseTask.save_json + safe_replace_file;
      completed-msg default template; class-constant thresholds default to
      MF values (0.98 / 0.5); pre-fetch dependency-check semantics = fetch
      failure returns (None, ...) and BaseRateTask.run() short-circuits to
      Failed WITHOUT clobbering last-good.

Behavioral invariants preserved for SetMFRate (regression floor)
=================================================================
* At success_ratio ≥ 98%, rates file writes are BIT-IDENTICAL to pre-refactor.
* Degenerate-answerable, hard-floor, and 98% partial-success gates match
  SetMFRate's pre-refactor error messages verbatim (same operator log
  language, same jobs.result strings, minus the "MF rate job:" prefix which
  becomes "<self.title>:").
* permanent_404 vs permanent_4xx split (ak-5jq v3 MINOR-B) preserved: MF's
  _fetch_all() still emits them as distinct classifications in
  permanent_skips_dict so the completed-message and denominator math match.

Behavioral additions for the 4 previously-ungated tasks
========================================================
* NPS: now guarded (empty-response clobber blocked) — floors set low
  (0.05) pending Phase 2 count-deviation baseline.
* PPF/EPF/Gold: fetch-failure and empty-response cases already returned
  Failed pre-refactor (universe-of-1 semantics); no behavioral change.
  Adds architectural consistency and blocks any future "silent green on
  empty payload" regression.
"""

from __future__ import annotations

import os
from abc import abstractmethod

from services.tasks.baseTask import BaseTask


class BaseRateTask(BaseTask):
    """Base class for coverage-gated rate-fetching tasks.

    Subclasses override:
      * `_rate_filename`           — filename under `self.tmp_dir` (e.g. 'MFRate.json').
      * `_rate_prefix_attr`        — attribute name on `self.jsonService` for the
                                     prefix (e.g. 'MfRatePrefix'). Follows the
                                     existing MF/NPS/PPF/EPF/Gold naming.
      * `_fetch_all()`             — abstract; returns the 5-tuple documented in
                                     the module docstring.
      * `_completed_msg(...)`      — optional override for a domain-specific
                                     Completed-status message. Default template
                                     lists ok / answerable and any permanent
                                     skip categories.

    Class-level tunables (subclasses may override):
      * `_MIN_SUCCESS_RATIO` (0.98) — 98% partial-success gate.
      * `_COVERAGE_HARD_FLOOR` (0.5) — hard floor below which last-good is
                                       preserved (write+swap SKIPPED).

    NB: matches SetMFRate's pre-refactor semantics EXACTLY at the default
    tunable values. Subclasses only need to change the tunables to reflect
    their own coverage math (e.g. NPS lowers both to 0.05 per Q2 option (a)).
    """

    # Default gate thresholds — subclasses may override.
    # ak-539 C1: 98% partial-success threshold. Runs below get status=Failed
    # even though the file is written (partial data still better than stale
    # for most callers).
    _MIN_SUCCESS_RATIO: float = 0.98
    # ak-539 v2 MAJOR: hard floor. Below this, both the tmp write AND the
    # safe_replace_file swap are SKIPPED so last-good stays on disk.
    _COVERAGE_HARD_FLOOR: float = 0.5

    # Subclasses MUST override these two:
    _rate_filename: str | None = None       # e.g. "MFRate.json"
    _rate_prefix_attr: str | None = None    # e.g. "MfRatePrefix"

    @abstractmethod
    def _fetch_all(self):
        """Return `(data_dict, total, ok, permanent_skips_dict, extras_dict)`.

        See module docstring for the full contract. Summary:
          * data_dict     : payload to write, or None on outright fetch failure.
          * total         : size of the fetch universe (>=0).
          * ok            : successful items (0 <= ok <= total).
          * permanent_skips_dict : {classification: count} excluded from
                                   the answerable denominator.
          * extras_dict   : {arbitrary_key: value} — reserved for
                            subclass-specific metadata. Base only reads
                            `extras.get('error')` for the fetch-failure msg
                            when data is None.
        """
        raise NotImplementedError

    # ---- optional overrides ----

    def _get_rate_prefix(self):
        """Resolve the prefix string from `_rate_prefix_attr` at runtime.

        Kept as a method (not a property) so subclasses can override with
        computed logic if the attribute name isn't the entire mapping.
        """
        if not self._rate_prefix_attr:
            raise TypeError(
                f"{type(self).__name__} must set _rate_prefix_attr "
                f"(attribute name on jsonService)"
            )
        return getattr(self.jsonService, self._rate_prefix_attr)

    def _completed_msg(self, ok, answerable, permanent_skips, extras):
        """Default Completed-status message. Subclasses may override for
        domain-specific formatting (e.g. SetMFRate wants the exact
        permanent-404 vs permanent-4xx split language).

        Default: 'Completed successfully' when there are no permanent
        skips (matches existing NPS/PPF/EPF/Gold literal); else appends
        the skip counts.
        """
        # Filter out zero-count classifications for the message shape.
        nz_skips = {k: v for k, v in (permanent_skips or {}).items() if v}
        if not nz_skips:
            return 'Completed successfully'
        skip_parts = ", ".join(f"{k}={v}" for k, v in sorted(nz_skips.items()))
        return f'Completed successfully ({ok}/{answerable} answerable; skips: {skip_parts})'

    def _fmt_degenerate_msg(self, total, permanent_skips):
        """Format the 'coverage degenerate' Failed-status message.

        Subclasses may override to preserve domain-specific phrasing
        (SetMFRate wants 'urls_total' + explicit permanent_404 /
        permanent_4xx breakdown + 'NAVs on disk'). Default keeps the
        generic form callers can rely on: the substrings 'coverage
        degenerate' + 'preserving last-good' are guaranteed."""
        return (
            f"coverage degenerate: answerable=0 "
            f"(total={total} permanent_skips={permanent_skips}); "
            f"preserving last-good on disk"
        )

    def _fmt_hard_floor_msg(self, ok, answerable, ratio):
        """Format the 'coverage below hard floor' Failed-status message.

        Subclasses may override to preserve domain-specific phrasing.
        Guaranteed substrings: 'coverage below hard floor', the ratio
        as ':.2%', 'preserving last-good'."""
        return (
            f"coverage below hard floor: {ok}/{answerable} answerable "
            f"({ratio:.2%} — below {self._COVERAGE_HARD_FLOOR:.0%} floor); "
            f"preserving last-good on disk"
        )

    def _fmt_partial_success_msg(self, ok, answerable, ratio):
        """Format the 'partial success' Failed-status message written to
        disk (above hard-floor, below _MIN_SUCCESS_RATIO).

        Subclasses may override to preserve domain-specific phrasing
        (SetMFRate wants '... schemes written ...'). Guaranteed
        substrings: 'partial success', the ratio as ':.2%'."""
        return (
            f"partial success: {ok}/{answerable} answerable "
            f"({ratio:.2%} — below {self._MIN_SUCCESS_RATIO:.0%} threshold)"
        )

    # ---- concrete run() — the gate machinery ----

    def run(self):
        try:
            fetched = self._fetch_all()
            data, total, ok, permanent_skips, extras = fetched

            # ak-2r8: fetch-failure fast-path. `_fetch_all` returns
            # `data is None` when it couldn't produce a payload at all
            # (upstream unreachable, dependency file missing, LLM
            # extract returned no schema-valid dict, etc.). Short-circuit
            # to Failed WITHOUT clobbering last-good.
            if data is None:
                err = (extras or {}).get('error') or 'fetch failed'
                # Some subclasses want the error truncated for jobs.result;
                # keep the base neutral and let extras['error'] be pre-truncated.
                return err, "Failed", self.interval

            permanent_skips = permanent_skips or {}
            total_perm_skips = sum(permanent_skips.values())
            answerable = max(int(total) - int(total_perm_skips), 0)

            # ak-5jq v2 MAJOR #2 + v3: degenerate-answerable guard.
            # answerable == 0 means every item in the universe was a
            # permanent skip (or the universe itself was empty). The
            # ratio would be a meaningless 1.0 by fallback; the on-disk
            # file would be either empty or last-good — either way
            # preserve last-good rather than clobber.
            if answerable == 0:
                msg = self._fmt_degenerate_msg(total, permanent_skips)
                self.logger.error(f"{self.title}: {msg}")
                return msg, "Failed", self.interval

            success_ratio = (ok / answerable) if answerable > 0 else 1.0

            # ak-539 v2 MAJOR: hard-floor gate FIRST. Below floor, both
            # the tmp write AND the swap are skipped so the previous
            # good file stays on disk exactly as-is. Callers keep
            # serving last-known rates until the next run recovers.
            if success_ratio < self._COVERAGE_HARD_FLOOR:
                msg = self._fmt_hard_floor_msg(ok, answerable, success_ratio)
                self.logger.error(f"{self.title}: {msg}")
                return msg, "Failed", self.interval

            # Above the hard floor — write + swap. The file may be
            # degraded (below 98%) but is still better than stale
            # for the majority of callers.
            filePath = os.path.join(self.tmp_dir, self._rate_filename)
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(data, filePath)

            swap_ok, swap_err = self.safe_replace_file(
                filePath, self._get_rate_prefix(), self.jsonService.ratesType
            )
            if not swap_ok:
                return swap_err, "Failed", self.interval

            # ak-539 C1: 98% partial-success gate. File written; status
            # reflects the degradation for operator visibility.
            if success_ratio < self._MIN_SUCCESS_RATIO:
                msg = self._fmt_partial_success_msg(ok, answerable, success_ratio)
                self.logger.warning(f"{self.title}: {msg}")
                return msg, "Failed", self.interval

            return self._completed_msg(ok, answerable, permanent_skips, extras or {}), "Completed", self.interval
        except Exception as ex:
            return str(ex), "Failed", self.interval
