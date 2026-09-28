package datahub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// A reviewed result must survive normal fact retention. New completed revisions
// produce a different ID and require renewed approval; deletion alone does not
// erase the evidence that was available at the time of review.
type StudyValidation struct {
	MultifactorComparison *MultifactorStudy   `json:"multifactorComparison,omitempty"`
	ID                    string              `json:"id"`
	StudyID               string              `json:"studyId"`
	Asset                 string              `json:"asset"`
	InputVersion          string              `json:"inputVersion"`
	Evaluation            string              `json:"evaluationVersion"`
	Rules                 string              `json:"rulesVersion"`
	From                  time.Time           `json:"from"`
	To                    time.Time           `json:"to"`
	CalculatedAt          time.Time           `json:"calculatedAt"`
	FlowCoverage          float64             `json:"flowCoverage"`
	CandleCoverage        float64             `json:"candleCoverage"`
	Comparison            CandidateComparison `json:"comparison"`
}

func (h *Hub) freezeStudyValidation(ctx context.Context, s Study, now time.Time) (string, error) {
	r := s.Result
	if r != nil && r.MultifactorComparison != nil && r.MultifactorComparison.State == "calculating" {
		return "", errors.New("不能冻结仍在计算的多因素研究")
	}
	if s.ID == "" || !researchAsset(s.Asset) || s.Pipeline != studyPipeline || s.InputVersion == "" || s.To.Sub(s.From) != 90*24*time.Hour || s.To.After(now) || r == nil || !r.CoreCalculated || r.Evaluation != EvaluationVersion || r.CandidateComparison == nil || r.CandidateComparison.Rules != CandidateRules || r.CandidateComparison.Evaluation != EvaluationVersion || r.FlowCoverage < .95 || r.CandleCoverage < .95 || r.BaselineDays != 30 || r.DevelopmentDays != 30 || r.HoldoutDays != 30 {
		return "", errors.New("不能冻结未完成的候选研究")
	}
	hash := sha256.Sum256([]byte(s.ID + "/" + s.InputVersion + "/" + EvaluationVersion + "/" + CandidateRules))
	id := fmt.Sprintf("validation-%x", hash[:12])
	v := StudyValidation{ID: id, StudyID: s.ID, Asset: s.Asset, InputVersion: s.InputVersion, Evaluation: EvaluationVersion, Rules: CandidateRules, From: s.From, To: s.To, CalculatedAt: now, FlowCoverage: r.FlowCoverage, CandleCoverage: r.CandleCoverage, Comparison: *r.CandidateComparison}
	v.MultifactorComparison = r.MultifactorComparison
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	if len(b) > 1<<20 {
		return "", errors.New("验证快照超过存储预算")
	}
	_, e = h.Store.research.ExecContext(ctx, "INSERT OR IGNORE INTO documents(kind,id,asset,at,payload) VALUES('study-validation',?,?,?,?)", id, s.Asset, now.Unix(), b)
	return id, e
}

func (h *Hub) StudyValidationView(ctx context.Context, asset, id string) (StudyValidation, error) {
	var v StudyValidation
	if e := h.Store.document(ctx, "study-validation", id, &v); e != nil {
		return v, e
	}
	if v.Asset != asset {
		return StudyValidation{}, errors.New("评估快照不属于该币种")
	}
	return v, nil
}
