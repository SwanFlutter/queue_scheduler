import 'package:flutter_test/flutter_test.dart';
import 'package:example/main.dart';

void main() {
  testWidgets('QueueSchedulerExampleApp smoke test', (WidgetTester tester) async {
    await tester.pumpWidget(const QueueSchedulerExampleApp());
    await tester.pumpAndSettle();
    expect(find.text('Download Queue & Scheduler'), findsOneWidget);
  });
}
