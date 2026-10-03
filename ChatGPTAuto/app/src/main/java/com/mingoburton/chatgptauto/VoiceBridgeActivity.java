package com.mingoburton.chatgptauto;

import android.app.Activity;
import android.content.ComponentName;
import android.content.Intent;
import android.os.Bundle;
import android.widget.Toast;

public class VoiceBridgeActivity extends Activity {

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";
    private static final String CHATGPT_VOICE_ACTIVITY =
            "com.openai.voice.assistant.AssistantActivity";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        launchVoice();
    }

    private void launchVoice() {
        try {
            Intent voice = new Intent();
            voice.setComponent(new ComponentName(
                    CHATGPT_PACKAGE,
                    CHATGPT_VOICE_ACTIVITY
            ));
            voice.addFlags(
                    Intent.FLAG_ACTIVITY_NEW_TASK |
                    Intent.FLAG_ACTIVITY_CLEAR_TOP |
                    Intent.FLAG_ACTIVITY_SINGLE_TOP
            );
            startActivity(voice);
        } catch (Exception e) {
            Toast.makeText(
                    this,
                    "ChatGPT Voice non può essere avviato direttamente su questa versione.",
                    Toast.LENGTH_LONG
            ).show();
        }
        finish();
    }
}
