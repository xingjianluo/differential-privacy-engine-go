package dpengine_test

import (
	"fmt"
	"log"

	dpengine "github.com/xingjianluo/differential-privacy-engine-go"
)

// ExampleCount 演示注册数据集、带贡献上界的 Laplace 计数查询与重复请求重放。
func ExampleEngine_Count() {
	engine, err := dpengine.NewEngine() // 纯内存模式；加 dpengine.WithStore(path) 开启持久化
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// 数据集总预算 epsilon=1。
	if err := engine.RegisterDataset("users", dpengine.PrivacyParams{Epsilon: 1}); err != nil {
		log.Fatal(err)
	}

	query := dpengine.CountQuery{
		Dataset:          "users",
		RequestID:        "unique-request-id-001", // 调用方生成的幂等键
		Params:           dpengine.PrivacyParams{Epsilon: 0.5},
		MaxContributions: 3,                    // 每用户最多计 3：L1 灵敏度
		Policy:           dpengine.PolicyClamp, // 超上界裁剪而非拒绝
		Contributions: map[string]int{
			"alice": 3,
			"bob":   1,
			"carol": 9, // 超出上界，裁剪为 3
		},
	}
	res, err := engine.Count(query)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("true=%d clamped=%d noisy=%.3f scale=%.1f\n",
		res.TrueValue, res.ClampedContributions, res.NoisyValue, res.NoiseScale)

	// 相同 requestID 重放：返回完全相同的结果，预算只计一次。
	replay, err := engine.Count(query)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replay identical=%v\n", replay.NoisyValue == res.NoisyValue)

	used, _, _ := engine.Used("users")
	fmt.Printf("used epsilon=%.1f\n", used)
	// Output:
	// true=7 clamped=1 noisy=-3.985 scale=6.0
	// replay identical=true
	// used epsilon=0.5
}

// ExampleParallelCount 演示互不相交分区上的并行组合：预算只按 max(eps) 计一次。
func ExampleEngine_ParallelCount() {
	engine, err := dpengine.NewEngine()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	if err := engine.RegisterDataset("users", dpengine.PrivacyParams{Epsilon: 1}); err != nil {
		log.Fatal(err)
	}

	results, err := engine.ParallelCount(dpengine.ParallelCountQuery{
		Dataset:   "users",
		RequestID: "parallel-001",
		Children: []dpengine.CountQuery{
			{
				Params:           dpengine.PrivacyParams{Epsilon: 0.3},
				MaxContributions: 1,
				Policy:           dpengine.PolicyClamp,
				Contributions:    map[string]int{"alice": 1},
				Partition:        "region-a",
			},
			{
				Params:           dpengine.PrivacyParams{Epsilon: 0.7},
				MaxContributions: 1,
				Policy:           dpengine.PolicyClamp,
				Contributions:    map[string]int{"bob": 1},
				Partition:        "region-b",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		fmt.Printf("partition=%s true=%d\n", r.Partition, r.TrueValue)
	}
	used, _, _ := engine.Used("users")
	fmt.Printf("used epsilon=%.1f (max, not sum)\n", used)
	// Output:
	// partition=region-a true=1
	// partition=region-b true=1
	// used epsilon=0.7 (max, not sum)
}

// ExampleErrorCodes 演示不同拒绝原因返回可区分的错误码。
func ExampleHasCode() {
	engine, err := dpengine.NewEngine()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	_, err = engine.Count(dpengine.CountQuery{
		Dataset: "ghost", RequestID: "x",
		Params: dpengine.PrivacyParams{Epsilon: 0.1}, MaxContributions: 1,
		Policy: dpengine.PolicyReject,
	})
	fmt.Println(dpengine.HasCode(err, dpengine.ErrDatasetNotFound))
	// Output: true
}
