import '../globals.css';

export default function WorkerRootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <title>layerd — TEE Worker Dashboard</title>
        <link rel="icon" href="/favicon.ico" sizes="any" />
      </head>
      <body className="bg-gray-950 text-gray-100">
        {children}
      </body>
    </html>
  );
}
