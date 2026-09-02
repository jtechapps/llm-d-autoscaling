package asyncqueue

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("parseParams", func() {
	It("returns all defaults for a nil map", func() {
		p, err := parseParams(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.mode).To(Equal(modeBinary))
		Expect(p.perReplicaCapacity).To(Equal(defaultPerReplicaCapacity))
		Expect(p.minQueueThreshold).To(Equal(defaultMinQueueThreshold))
		Expect(p.minScaleDownAge).To(Equal(defaultMinScaleDownAge))
	})

	It("parses the M1 keys from the roadmap schema", func() {
		p, err := parseParams(map[string]any{
			"mode":                 "binary",
			"defaultPerReplicaRPS": 20.0,
			"minQueueThreshold":    5,
			"minScaleDownAge":      "30m",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.mode).To(Equal("binary"))
		Expect(p.perReplicaCapacity).To(Equal(20.0))
		Expect(p.minQueueThreshold).To(Equal(5.0))
		Expect(p.minScaleDownAge).To(Equal(30 * time.Minute))
	})

	It("accepts a bare number of seconds for minScaleDownAge", func() {
		p, err := parseParams(map[string]any{"minScaleDownAge": 90})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.minScaleDownAge).To(Equal(90 * time.Second))
	})

	It("tolerates unknown keys belonging to later milestones", func() {
		p, err := parseParams(map[string]any{
			"deadlineQuantile": 0.10,
			"maxReplicas":      5,
			"quota":            map[string]any{"enabled": true},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.perReplicaCapacity).To(Equal(defaultPerReplicaCapacity))
	})

	It("rejects a non-binary mode as unsupported in M1", func() {
		_, err := parseParams(map[string]any{"mode": "deadline_aware"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Milestone 1"))
	})

	It("rejects a non-positive defaultPerReplicaRPS", func() {
		_, err := parseParams(map[string]any{"defaultPerReplicaRPS": 0})
		Expect(err).To(HaveOccurred())
	})

	It("rejects a negative minQueueThreshold", func() {
		_, err := parseParams(map[string]any{"minQueueThreshold": -1})
		Expect(err).To(HaveOccurred())
	})

	It("rejects a negative minScaleDownAge", func() {
		_, err := parseParams(map[string]any{"minScaleDownAge": "-5m"})
		Expect(err).To(HaveOccurred())
	})

	It("rejects a wrongly-typed known key", func() {
		_, err := parseParams(map[string]any{"defaultPerReplicaRPS": "twelve"})
		Expect(err).To(HaveOccurred())
	})

	It("rejects an unparsable duration", func() {
		_, err := parseParams(map[string]any{"minScaleDownAge": "soon"})
		Expect(err).To(HaveOccurred())
	})
})
