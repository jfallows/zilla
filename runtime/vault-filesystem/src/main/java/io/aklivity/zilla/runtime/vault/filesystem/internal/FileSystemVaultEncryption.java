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

import java.security.SecureRandom;
import java.util.function.Function;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;

import io.aklivity.zilla.runtime.engine.model.function.ValueConsumer;

public class FileSystemVaultEncryption
{
    private static final String AES_GCM_CIPHER = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private final Function<String, SecretKey> supplySecretKey;
    private final SecureRandom random;
    private final MutableDirectBuffer encryptBuffer;
    private final byte[] iv;

    private byte[] workBuffer;
    private final Cipher encryptCipher;
    private final Cipher decryptCipher;

    FileSystemVaultEncryption(
        Function<String, SecretKey> supplySecretKey)
    {
        this.supplySecretKey = supplySecretKey;
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

    public int encrypt(
        String keyRef,
        DirectBuffer plaintext,
        int index,
        int length,
        DirectBuffer aad,
        int aadIndex,
        int aadLength,
        ValueConsumer output)
    {
        int result = -1;

        SecretKey secretKey = supplySecretKey.apply(keyRef);
        if (secretKey != null)
        {
            try
            {
                random.nextBytes(iv);
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
                encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
                if (aad != null)
                {
                    encryptCipher.updateAAD(aad.byteArray(), aad.wrapAdjustment() + aadIndex, aadLength);
                }
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

    public int decrypt(
        String keyRef,
        DirectBuffer ciphertext,
        int index,
        int length,
        DirectBuffer aad,
        int aadIndex,
        int aadLength,
        ValueConsumer output)
    {
        int result = -1;

        SecretKey secretKey = supplySecretKey.apply(keyRef);
        if (secretKey != null && length > GCM_IV_LENGTH)
        {
            try
            {
                ciphertext.getBytes(index, iv, 0, GCM_IV_LENGTH);
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
                decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
                if (aad != null)
                {
                    decryptCipher.updateAAD(aad.byteArray(), aad.wrapAdjustment() + aadIndex, aadLength);
                }
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
}
