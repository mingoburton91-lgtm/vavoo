package com.mingoburton.chatgptauto;

import android.app.Activity;
import android.content.ComponentName;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.widget.Toast;

public class VoiceLaunchActivity extends Activity {

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";
    private static final String VOICE_ACTIVITY = "com.openai.voice.assistant.AssistantActivity";
    private static final String MAIN_ACTIVITY = "com.openai.chatgpt.MainActivity";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        launchChatGptVoice();
    }

    private void launchChatGptVoice() {
        PackageManager pm = getPackageManager();

        // 1) Direct voice mode. Explicit component: no browser can handle this.
        Intent voice = new Intent();
        voice.setComponent(new ComponentName(CHATGPT_PACKAGE, VOICE_ACTIVITY));
        voice.addFlags(
                Intent.FLAG_ACTIVITY_NEW_TASK |
                Intent.FLAG_ACTIVITY_CLEAR_TOP |
                Intent.FLAG_ACTIVITY_SINGLE_TOP
        );

        if (voice.resolveActivity(pm) != null) {
            try {
                startActivity(voice);
                finish();
                return;
            } catch (Exception ignored) {
            }
        }

        // 2) Fallback to the native ChatGPT MainActivity only.
        // Still explicit: this can never resolve to Samsung Internet.
        Intent main = new Intent(Intent.ACTION_MAIN);
        main.setComponent(new ComponentName(CHATGPT_PACKAGE, MAIN_ACTIVITY));
        main.addCategory(Intent.CATEGORY_LAUNCHER);
        main.addFlags(
                Intent.FLAG_ACTIVITY_NEW_TASK |
                Intent.FLAG_ACTIVITY_CLEAR_TOP
        );

        if (main.resolveActivity(pm) != null) {
            try {
                Toast.makeText(
                        this,
                        "Voce diretta non disponibile: apro l'app ChatGPT",
                        Toast.LENGTH_SHORT
                ).show();
                startActivity(main);
                finish();
                return;
            } catch (Exception ignored) {
            }
        }

        Toast.makeText(
                this,
                "Impossibile trovare l'app ChatGPT ufficiale",
                Toast.LENGTH_LONG
        ).show();
        finish();
    }
}
