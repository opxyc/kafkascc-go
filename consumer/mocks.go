package consumer

type testLogger struct{}

func newTestLogger() Logger {
	return &testLogger{}
}

func (l *testLogger) With(args ...any) Logger {
	return l
}

func (l *testLogger) Info(msg string, args ...any) {}

func (l *testLogger) Error(msg string, args ...any) {}
