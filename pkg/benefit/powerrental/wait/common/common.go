package common

import (
	"fmt"
	"os"
	"time"
)

type Handler struct {
	benefitInterval  time.Duration
	nextBenefitAt    time.Time
	benefitTimestamp uint32
}

func NewHandler() *Handler {
	h := &Handler{
		benefitInterval: 24 * time.Hour,
	}
	h.prepareInterval()
	return h
}

func (h *Handler) SetBenefitIntervalHours(benefitIntervalHours uint32) {
	h.benefitInterval = time.Duration(benefitIntervalHours) * time.Hour
}

func (h *Handler) prepareInterval() {
	if duration, err := time.ParseDuration(
		fmt.Sprintf("%vs", os.Getenv("ENV_BENEFIT_INTERVAL_SECONDS"))); err == nil && duration > 0 {
		h.benefitInterval = duration
	}
	h.CalculateNextBenefitAt()
}

func (h *Handler) BenefitInterval() time.Duration {
	return h.benefitInterval
}

func (h *Handler) BenefitTimestamp() uint32 {
	return h.benefitTimestamp
}

func (h *Handler) NextBenefitAt() time.Time {
	return h.nextBenefitAt
}

func (h *Handler) CalculateNextBenefitAt() {
	now := time.Now()
	nowSec := now.Unix()
	benefitSeconds := int64(h.benefitInterval.Seconds())
	nextSec := (nowSec + benefitSeconds) / benefitSeconds * benefitSeconds
	h.nextBenefitAt = now.Add(time.Duration(nextSec-nowSec) * time.Second)
	h.benefitTimestamp = h.BenefitTimestampAt(uint32(time.Now().Unix()))
}

func (h *Handler) BenefitTimestampAt(timestamp uint32) uint32 {
	intervalFloat := h.benefitInterval.Seconds()
	intervalUint := uint32(intervalFloat)
	return timestamp / intervalUint * intervalUint
}
