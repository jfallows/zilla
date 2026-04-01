/*
 * Copyright 2021-2024 Aklivity Inc.
 *
 * Aklivity licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.aklivity.zilla.runtime.vault.filesystem.internal;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStore.Entry;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStore.SecretKeyEntry;
import java.security.KeyStore.TrustedCertificateEntry;
import java.security.SecureRandom;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;

import io.aklivity.zilla.runtime.engine.model.function.ValueConsumer;
import io.aklivity.zilla.runtime.engine.security.RevocationStrategy;
import io.aklivity.zilla.runtime.engine.vault.VaultHandler;
import io.aklivity.zilla.runtime.vault.filesystem.config.FileSystemOptionsConfig;
import io.aklivity.zilla.runtime.vault.filesystem.config.FileSystemStoreConfig;

public class FileSystemVaultHandler implements VaultHandler
{
    private static final String STORE_TYPE_DEFAULT = "pkcs12";
    private static final String PKIX_ALGORITHM = "PKIX";
    private static final String AES_GCM_CIPHER = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private final Function<List<String>, KeyManagerFactory> supplyKeys;
    private final Function<List<String>, KeyManagerFactory> supplySigners;
    private final BiFunction<List<String>, KeyStore, TrustManagerFactory> supplyTrust;
    private final RevocationStrategy revocation;
    private final FileSystemStoreInfo encryptKeys;
    private final SecureRandom random;
    private final MutableDirectBuffer encryptBuffer;
    private final byte[] iv;
    private byte[] workBuffer;
    private final Cipher encryptCipher;
    private final Cipher decryptCipher;

    public FileSystemVaultHandler(
        FileSystemOptionsConfig options,
        Function<String, Path> resolvePath)
    {
        this(options, resolvePath, RevocationStrategy.NONE);
    }

    public FileSystemVaultHandler(
        FileSystemOptionsConfig options,
        Function<String, Path> resolvePath,
        RevocationStrategy revocation)
    {
        FileSystemStoreInfo keys = supplyStoreInfo(resolvePath, options.keys);
        supplyKeys = keys != null
            ? keys::newKeysFactory
            : aliases -> null;

        FileSystemStoreInfo signers = supplyStoreInfo(resolvePath, options.signers);
        supplySigners = signers != null && keys != null
            ? aliases -> newSignersFactory(aliases, signers, keys)
            : aliases -> null;

        this.revocation = options.revocation != null ? options.revocation : revocation;
        FileSystemStoreInfo trust = supplyStoreInfo(resolvePath, options.trust);
        supplyTrust = (aliases, cacerts) -> newTrustFactory(trust, aliases, cacerts);

        this.encryptKeys = keys;
        if (keys != null)
        {
            this.random = new SecureRandom();
            this.encryptBuffer = new ExpandableArrayBuffer();
            this.iv = new byte[GCM_IV_LENGTH];
            this.workBuffer = new byte[256];
            Cipher ec = null;
            Cipher dc = null;
            try
            {
                ec = Cipher.getInstance(AES_GCM_CIPHER);
                dc = Cipher.getInstance(AES_GCM_CIPHER);
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
            this.encryptCipher = ec;
            this.decryptCipher = dc;
        }
        else
        {
            this.random = null;
            this.encryptBuffer = null;
            this.iv = null;
            this.workBuffer = null;
            this.encryptCipher = null;
            this.decryptCipher = null;
        }
    }

    @Override
    public KeyManagerFactory initKeys(
        List<String> aliases)
    {
        return supplyKeys.apply(aliases);
    }

    @Override
    public TrustManagerFactory initTrust(
        List<String> aliases,
        KeyStore cacerts)
    {
        return supplyTrust.apply(aliases, cacerts);
    }

    @Override
    public KeyManagerFactory initSigners(
        List<String> aliases)
    {
        return supplySigners.apply(aliases);
    }

    @Override
    public int encrypt(
        String keyRef,
        DirectBuffer plaintext,
        int index,
        int length,
        ValueConsumer output)
    {
        int result = -1;

        SecretKey secretKey = encryptKeys != null ? encryptKeys.secretKey(keyRef) : null;
        if (secretKey != null)
        {
            try
            {
                random.nextBytes(iv);
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
                encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
                if (workBuffer.length < length)
                {
                    workBuffer = new byte[length];
                }
                plaintext.getBytes(index, workBuffer, 0, length);
                final int outputLength = GCM_IV_LENGTH + encryptCipher.getOutputSize(length);
                encryptBuffer.checkLimit(outputLength);
                System.arraycopy(iv, 0, encryptBuffer.byteArray(), 0, GCM_IV_LENGTH);
                result = GCM_IV_LENGTH +
                    encryptCipher.doFinal(workBuffer, 0, length, encryptBuffer.byteArray(), GCM_IV_LENGTH);
                output.accept(encryptBuffer, 0, result);
            }
            catch (Exception ex)
            {
            }
        }

        return result;
    }

    @Override
    public int decrypt(
        String keyRef,
        DirectBuffer ciphertext,
        int index,
        int length,
        ValueConsumer output)
    {
        int result = -1;

        SecretKey secretKey = encryptKeys != null ? encryptKeys.secretKey(keyRef) : null;
        if (secretKey != null && length > GCM_IV_LENGTH)
        {
            try
            {
                ciphertext.getBytes(index, iv, 0, GCM_IV_LENGTH);
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
                decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
                final int ciphertextBodyLength = length - GCM_IV_LENGTH;
                if (workBuffer.length < ciphertextBodyLength)
                {
                    workBuffer = new byte[ciphertextBodyLength];
                }
                ciphertext.getBytes(index + GCM_IV_LENGTH, workBuffer, 0, ciphertextBodyLength);
                final int outputLength = decryptCipher.getOutputSize(ciphertextBodyLength);
                encryptBuffer.checkLimit(outputLength);
                result = decryptCipher.doFinal(workBuffer, 0, ciphertextBodyLength,
                    encryptBuffer.byteArray(), 0);
                output.accept(encryptBuffer, 0, result);
            }
            catch (Exception ex)
            {
            }
        }

        return result;
    }

    private static FileSystemStoreInfo supplyStoreInfo(
        Function<String, Path> resolvePath,
        FileSystemStoreConfig config)
    {
        FileSystemStoreInfo info = null;

        if (config != null)
        {
            try
            {
                Path storePath = resolvePath.apply(config.store);
                try (InputStream input = Files.newInputStream(storePath))
                {
                    String type = Optional.ofNullable(config.type).orElse(STORE_TYPE_DEFAULT);
                    char[] password = Optional.ofNullable(config.password).map(String::toCharArray).orElse(null);

                    KeyStore store = KeyStore.getInstance(type);
                    store.load(input, password);

                    info = new FileSystemStoreInfo(store, password);
                }
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return info;
    }

    private KeyManagerFactory newSignersFactory(
        List<String> aliases,
        FileSystemStoreInfo signers,
        FileSystemStoreInfo keys)
    {
        KeyManagerFactory factory = null;

        if (aliases != null)
        {
            factory = keys.newKeysFactory(aliases.stream()
                .map(signers::certificate)
                .filter(Objects::nonNull)
                .map(TrustedCertificateEntry::getTrustedCertificate)
                .filter(X509Certificate.class::isInstance)
                .map(X509Certificate.class::cast)
                .map(X509Certificate::getSubjectX500Principal)
                .map(keys::issuedKeys)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .toList());
        }

        return factory;
    }

    private TrustManagerFactory newTrustFactory(
        FileSystemStoreInfo store,
        List<String> aliases,
        KeyStore cacerts)
    {
        TrustManagerFactory factory = null;

        try
        {
            if (aliases != null || cacerts != null)
            {
                KeyStore trust = KeyStore.getInstance(STORE_TYPE_DEFAULT);
                trust.load(null, null);

                if (aliases != null && store != null)
                {
                    for (String alias : aliases)
                    {
                        TrustedCertificateEntry cert = store.certificate(alias);
                        if (cert != null)
                        {
                            trust.setEntry(alias, cert, null);
                        }
                    }
                }

                if (cacerts != null)
                {
                    for (String alias : aliases)
                    {
                        TrustedCertificateEntry cacert = FileSystemStoreInfo.certificate(cacerts, alias);
                        if (cacert != null)
                        {
                            trust.setEntry(alias, cacert, null);
                        }
                    }
                }

                switch (revocation)
                {
                case CRL:
                    factory = TrustManagerFactory.getInstance(PKIX_ALGORITHM);
                    PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trust, new X509CertSelector());
                    pkixParams.setRevocationEnabled(true);

                    CertPathValidator validator = CertPathValidator.getInstance(PKIX_ALGORITHM);
                    PKIXRevocationChecker checker = (PKIXRevocationChecker) validator.getRevocationChecker();
                    checker.setOptions(EnumSet.of(
                        PKIXRevocationChecker.Option.PREFER_CRLS
                    ));
                    pkixParams.addCertPathChecker(checker);

                    CertPathTrustManagerParameters tmParams = new CertPathTrustManagerParameters(pkixParams);
                    factory.init(tmParams);
                    break;
                default:
                    factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    factory.init(trust);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return factory;
    }

    private static final class FileSystemStoreInfo
    {
        private final KeyStore store;
        private final KeyStore.PasswordProtection protection;

        private FileSystemStoreInfo(
            KeyStore store,
            char[] password)
        {
            this.store = store;
            this.protection = password != null ? new KeyStore.PasswordProtection(password) : null;
        }

        private KeyManagerFactory newKeysFactory(
            List<String> aliases)
        {
            KeyManagerFactory factory = null;

            try
            {
                if (aliases != null)
                {
                    KeyStore keys = KeyStore.getInstance(STORE_TYPE_DEFAULT);
                    keys.load(null, protection.getPassword());

                    for (String alias : aliases)
                    {
                        PrivateKeyEntry key = key(alias);
                        if (key != null)
                        {
                            keys.setEntry(alias, key, protection);
                        }
                    }

                    factory = KeyManagerFactory.getInstance("PKIX");
                    factory.init(keys, protection.getPassword());
                }
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return factory;
        }

        private PrivateKeyEntry key(
            String alias)
        {
            return entry(store, protection, alias, PrivateKeyEntry.class);
        }

        private SecretKey secretKey(
            String alias)
        {
            SecretKeyEntry entry = entry(store, protection, alias, SecretKeyEntry.class);
            return entry != null ? entry.getSecretKey() : null;
        }

        private TrustedCertificateEntry certificate(
            String alias)
        {
            return entry(store, null, alias, TrustedCertificateEntry.class);
        }

        private List<String> issuedKeys(
            X500Principal issuer)
        {
            List<String> keys = null;

            try
            {
                List<String> candidateKeys = Collections.list(store.aliases()).stream()
                    .filter(alias -> issuedKey(alias, issuer))
                    .toList();

                keys = candidateKeys.isEmpty() ? null : candidateKeys;
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return keys;
        }

        private boolean issuedKey(
            String alias,
            X500Principal issuer)
        {
            PrivateKeyEntry key = key(alias);
            Certificate certificate = key != null ? key.getCertificate() : null;
            return certificate != null &&
                certificate instanceof X509Certificate &&
                issuer.equals(((X509Certificate) certificate).getIssuerX500Principal());
        }

        private static TrustedCertificateEntry certificate(
            KeyStore store,
            String alias)
        {
            return entry(store, null, alias, TrustedCertificateEntry.class);
        }

        private static <T extends Entry> T entry(
            KeyStore store,
            KeyStore.PasswordProtection protection,
            String alias,
            Class<T> type)
        {
            T typed = null;

            try
            {
                Entry entry = store.getEntry(alias, protection);
                if (type.isInstance(entry))
                {
                    typed = type.cast(entry);
                }
            }
            catch (GeneralSecurityException ex)
            {
            }

            return typed;
        }
    }
}
