package asyncqueue

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAsyncQueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Async Queue Analyzer Suite")
}
