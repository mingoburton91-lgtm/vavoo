package com.mingoburton.chatgptauto;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.widget.Toast;

public class VoiceBridgeActivity extends Activity {

    private static final String CHATGPT_PACKAGE = "com.openai.chatgpt";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        launchChatGptFresh();
    }

    private void launchChatGptFresh() {
        try {
            Intent launch = getPackageManager().getLaunchIntentForPackage(CHATGPT_PACKAGE);

            if (launch == null) {
                Toast.makeText(
                        this,
                        "App ChatGPT ufficiale non trovata.",
                        Toast.LENGTH_LONG
                ).show();
                finish();
                return;
            }

            // Start the official ChatGPT app as a fresh task.
            // If ChatGPT Settings > Voice > Start with Voice is enabled,
            // ChatGPT itself should start Voice on a new/empty conversation.
            launch.addFlags(
                    Intent.FLAG_ACTIVITY_NEW_TASK |
                    Intent.FLAG_ACTIVITY_CLEAR_TASK |
                    Intent.FLAG_ACTIVITY_RESET_TASK_IF_NEEDED
            );

            startActivity(launch);
        } catch (Exception e) {
            Toast.makeText(
                    this,
                    "Impossibile aprire l'app ChatGPT.",
                    Toast.LENGTH_LONG
            ).show();
        }

        finish();
    }
}
