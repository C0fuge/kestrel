package pro.surpin.data.arrowflight.server.storage;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;

/**
 * A self-signed certificate for 127.0.0.1 and localhost, with the server TLS context that
 * presents it.
 *
 * @param certificate the certificate
 * @param serverContext TLS context of a server presenting it
 */
record TestTls(X509Certificate certificate, SSLContext serverContext) {

    /**
     * Creates a new key pair and certificate.
     *
     * @param commonName subject common name
     * @return certificate and server context
     * @throws Exception when the JDK cannot create them
     */
    static TestTls create(String commonName) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        X500Name name = new X500Name("CN=" + commonName);
        Instant now = Instant.now();
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(name,
                BigInteger.valueOf(now.toEpochMilli()), Date.from(now.minus(1, ChronoUnit.HOURS)),
                Date.from(now.plus(1, ChronoUnit.DAYS)), name, keys.getPublic());
        builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(
                new GeneralName[] {new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
                    new GeneralName(GeneralName.dNSName, "localhost")}));
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate())));
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        char[] password = "changeit".toCharArray();
        keyStore.setKeyEntry("server", keys.getPrivate(), password, new Certificate[] {certificate});
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keyStore, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), null, null);
        return new TestTls(certificate, context);
    }

    /**
     * Writes the certificate as PEM.
     *
     * @param file target file
     * @return the file
     * @throws IOException when it cannot be written
     */
    Path writePem(Path file) throws IOException {
        try {
            String pem = "-----BEGIN CERTIFICATE-----\n"
                    + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                            .encodeToString(certificate.getEncoded())
                    + "\n-----END CERTIFICATE-----\n";
            return Files.writeString(file, pem);
        } catch (java.security.cert.CertificateEncodingException e) {
            throw new IOException(e);
        }
    }
}
