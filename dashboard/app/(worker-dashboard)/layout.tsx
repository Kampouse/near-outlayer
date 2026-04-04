import '../globals.css';

export default function WorkerRootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" style={{background:'#030712',color:'#f3f4f6'}}>
      <head>
        <title>layerd — TEE Worker Dashboard</title>
      </head>
      <body className="bg-gray-950 text-gray-100" style={{margin:0,padding:0,background:'#030712',minHeight:'100vh',overflow:'auto'}}>
        {children}
      </body>
    </html>
  );
}
