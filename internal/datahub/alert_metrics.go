package datahub

// Shared machine-readable definitions accompany existing numeric fields without
// changing their old denominators or mixing descriptive and account performance.
func alertMetricDefinitions() map[string]any {
	return map[string]any{
		"priceCoverage":      "已登记行情事件的一对一提前/跟随/未匹配计数；分母为行情事件，不是成交笔数",
		"directionReturn":    "买盘上涨、卖压下跌为正；按各研究规则的可用时间和闭合现货K线计算；不含合约点差、手续费或资金费",
		"positiveReturnRate": "分母仅为该期限已成熟且价格路径完整的方向观察样本；未知和未成熟不按负收益或零收益处理",
		"paperAll":           "原实验全部已平仓及异常交易；待资金费完整净收益未知，异常不从账户总账删除",
		"paperComplete":      "已平仓、资金费已取得且无路径质量标记的成本后样本；独立于全部账户结果",
		"paperCoverage":      "coveredSeconds / observedSeconds，包含中断时间；collect模式停止新开仓，不改变原起点",
		"drawdown":           "连续片段观测值、跨已知净值点下界与完整采样回撤分别报告；缺口不连线，未知路径不声称完整回撤",
		"paperJudgmentGate":  map[string]any{"days": 30, "closedPerGroup": 100, "longPerGroup": 30, "shortPerGroup": 30, "coveragePercent": 95, "method": "按日分块重采样净期望区间、同一成交路径费用加倍敏感性；不据高胜率宣布可实盘"},
		"layeredCandidate":   map[string]any{"rulesVersion": LayeredRules, "maximumDirectionalDisplacementAtr": "1.5", "priceConfirmation": "父事件自身两根完整5分钟突破", "flowWindowsMinutes": []int{60, 240}, "maximumInputAgeSeconds": 720, "retroactiveEnrollment": false, "paperEntry": false, "mail": false},
	}
}
