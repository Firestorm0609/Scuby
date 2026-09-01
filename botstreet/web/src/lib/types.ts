export interface Holding {
	ticker: string;
	name: string;
	dollars: number;
	shares: number;
	avg_entry_price: number;
	current_price: number;
	unrealized_pnl_pct: number;
}

export interface Portfolio {
	agent: string;
	updated_at: string;
	starting_balance: number;
	cash: number;
	holdings: Holding[];
	total_value: number;
	all_time_return_pct: number;
	day_number: number;
	start_date: string;
	last_action: { summary: string; timestamp: string } | null;
	last_run_date: string | null;
}

export interface TradeRecord {
	action: 'buy' | 'sell';
	ticker: string;
	amount: number;
	price: number;
	shares: number;
	reason?: string;
}

export interface HistorySnapshot {
	date: string;
	day_number: number;
	total_value: number;
	cash: number;
	all_time_return_pct: number;
	holdings: { ticker: string; name: string; dollars: number; shares: number; current_price: number }[];
	trades: TradeRecord[];
}

export interface MarketQuote {
	ticker: string;
	name: string;
	price: number;
	change_pct: number;
}
